package io.github.gabrielshanahan.scoop.shared.configuration

import io.quarkus.credentials.CredentialsProvider
import io.quarkus.credentials.runtime.CredentialsProviderFinder
import io.quarkus.datasource.common.runtime.DataSourceUtil
import io.quarkus.datasource.runtime.DataSourceRuntimeConfig
import io.quarkus.datasource.runtime.DataSourcesRuntimeConfig
import io.quarkus.reactive.datasource.runtime.DataSourceReactiveRuntimeConfig
import io.quarkus.reactive.datasource.runtime.DataSourcesReactiveRuntimeConfig
import io.quarkus.reactive.pg.client.runtime.DataSourceReactivePostgreSQLConfig
import io.quarkus.reactive.pg.client.runtime.DataSourcesReactivePostgreSQLConfig
import io.quarkus.vertx.core.runtime.SSLConfigHelper
import io.vertx.core.Vertx
import io.vertx.pgclient.PgConnectOptions
import io.vertx.pgclient.SslMode
import io.vertx.pgclient.pubsub.PgSubscriber
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import java.util.function.Consumer
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.jboss.logging.Logger

@ApplicationScoped
class PgSubscriberProducer(
    private val vertx: Vertx,
    private val dataSourcesRuntimeConfig: DataSourcesRuntimeConfig,
    private val dataSourcesReactiveRuntimeConfig: DataSourcesReactiveRuntimeConfig,
    private val dataSourcesReactivePostgreSQLConfig: DataSourcesReactivePostgreSQLConfig,
) {

    companion object {
        private val logger = Logger.getLogger(PgSubscriberProducer::class.java)
    }

    @Produces
    @ApplicationScoped
    fun pgSubscriber(): PgSubscriber {
        val datasourceName = DataSourceUtil.DEFAULT_DATASOURCE_NAME
        val options =
            pgConnectOptions(
                    datasourceName,
                    dataSourcesRuntimeConfig.dataSources().getValue(datasourceName),
                    dataSourcesReactiveRuntimeConfig
                        .dataSources()
                        .getValue(datasourceName)
                        .reactive(),
                    dataSourcesReactivePostgreSQLConfig
                        .dataSources()
                        .getValue(datasourceName)
                        .reactive()
                        .postgresql(),
                )
                .first()

        return runBlocking {
            PgSubscriber.subscriber(vertx, options).apply {
                connect()
                    .onSuccess { logger.info("Subscribed to PgSubscriber") }
                    .onFailure { logger.error("Failed to subscribe to PgSubscriber", it) }
                    .toCompletionStage()
                    .await()
            }
        }
    }

    /**
     * Taken verbatim (plus AI fixes so detekt won't complain) from
     * [io.quarkus.reactive.pg.client.runtime.PgPoolRecorder.toPgConnectOptions].
     */
    private fun pgConnectOptions(
        dataSourceName: String,
        dataSourceRuntimeConfig: DataSourceRuntimeConfig,
        dataSourceReactiveRuntimeConfig: DataSourceReactiveRuntimeConfig,
        dataSourceReactivePostgreSQLConfig: DataSourceReactivePostgreSQLConfig,
    ): List<PgConnectOptions> {
        val pgConnectOptionsList: MutableList<PgConnectOptions> = mutableListOf()

        if (dataSourceReactiveRuntimeConfig.url().isPresent) {
            val urls = dataSourceReactiveRuntimeConfig.url().get()
            urls.forEach(
                Consumer { url: String ->
                    var url = url
                    // clean up the URL to make migrations easier
                    if (url.matches("^vertx-reactive:postgre(?:s|sql)://.*$".toRegex())) {
                        url = url.substring("vertx-reactive:".length)
                    }
                    pgConnectOptionsList.add(PgConnectOptions.fromUri(url))
                }
            )
        } else {
            pgConnectOptionsList.add(PgConnectOptions())
        }

        pgConnectOptionsList.forEach(
            Consumer { pgConnectOptions: PgConnectOptions ->
                configureAuthentication(pgConnectOptions, dataSourceRuntimeConfig)
                configureConnection(
                    pgConnectOptions,
                    dataSourceReactiveRuntimeConfig,
                    dataSourceReactivePostgreSQLConfig,
                )
                configureSSL(
                    pgConnectOptions,
                    dataSourceReactiveRuntimeConfig,
                    dataSourceReactivePostgreSQLConfig,
                )
                configureReconnection(pgConnectOptions, dataSourceReactiveRuntimeConfig)
                configureMetrics(pgConnectOptions, dataSourceName)
            }
        )

        return pgConnectOptionsList
    }

    private fun configureAuthentication(
        pgConnectOptions: PgConnectOptions,
        dataSourceRuntimeConfig: DataSourceRuntimeConfig,
    ) {
        dataSourceRuntimeConfig.username().ifPresent { user: String? ->
            pgConnectOptions.setUser(user)
        }
        dataSourceRuntimeConfig.password().ifPresent { password: String? ->
            pgConnectOptions.setPassword(password)
        }

        // credentials provider
        if (dataSourceRuntimeConfig.credentialsProvider().isPresent) {
            val beanName = dataSourceRuntimeConfig.credentialsProviderName().orElse(null)
            val credentialsProvider = CredentialsProviderFinder.find(beanName)
            val name = dataSourceRuntimeConfig.credentialsProvider().get()
            val credentials = credentialsProvider.getCredentials(name)
            val user = credentials[CredentialsProvider.USER_PROPERTY_NAME]
            val password = credentials[CredentialsProvider.PASSWORD_PROPERTY_NAME]
            if (user != null) {
                pgConnectOptions.setUser(user)
            }
            if (password != null) {
                pgConnectOptions.setPassword(password)
            }
        }
    }

    private fun configureConnection(
        pgConnectOptions: PgConnectOptions,
        dataSourceReactiveRuntimeConfig: DataSourceReactiveRuntimeConfig,
        dataSourceReactivePostgreSQLConfig: DataSourceReactivePostgreSQLConfig,
    ) {
        pgConnectOptions.setCachePreparedStatements(
            dataSourceReactiveRuntimeConfig.cachePreparedStatements()
        )

        if (dataSourceReactivePostgreSQLConfig.pipeliningLimit().isPresent) {
            pgConnectOptions.setPipeliningLimit(
                dataSourceReactivePostgreSQLConfig.pipeliningLimit().asInt
            )
        }

        pgConnectOptions.setUseLayer7Proxy(dataSourceReactivePostgreSQLConfig.useLayer7Proxy())

        dataSourceReactiveRuntimeConfig.additionalProperties().forEach {
            (key: String?, value: String?) ->
            pgConnectOptions.addProperty(key, value)
        }
    }

    private fun configureSSL(
        pgConnectOptions: PgConnectOptions,
        dataSourceReactiveRuntimeConfig: DataSourceReactiveRuntimeConfig,
        dataSourceReactivePostgreSQLConfig: DataSourceReactivePostgreSQLConfig,
    ) {
        if (dataSourceReactivePostgreSQLConfig.sslMode().isPresent) {
            val sslMode = dataSourceReactivePostgreSQLConfig.sslMode().get()
            pgConnectOptions.setSslMode(sslMode)

            val algo = dataSourceReactiveRuntimeConfig.hostnameVerificationAlgorithm()
            // If sslMode is verify-full, we also need a hostname verification algorithm
            require(!("NONE".equals(algo, ignoreCase = true) && sslMode == SslMode.VERIFY_FULL)) {
                "quarkus.datasource.reactive.hostname-verification-algorithm must be specified " +
                    "under verify-full sslmode"
            }
        }

        pgConnectOptions.setTrustAll(dataSourceReactiveRuntimeConfig.trustAll())

        SSLConfigHelper.configurePemTrustOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.trustCertificatePem(),
        )
        SSLConfigHelper.configureJksTrustOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.trustCertificateJks(),
        )
        SSLConfigHelper.configurePfxTrustOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.trustCertificatePfx(),
        )

        SSLConfigHelper.configurePemKeyCertOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.keyCertificatePem(),
        )
        SSLConfigHelper.configureJksKeyCertOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.keyCertificateJks(),
        )
        SSLConfigHelper.configurePfxKeyCertOptions(
            pgConnectOptions,
            dataSourceReactiveRuntimeConfig.keyCertificatePfx(),
        )

        val algo = dataSourceReactiveRuntimeConfig.hostnameVerificationAlgorithm()
        if ("NONE".equals(algo, ignoreCase = true)) {
            pgConnectOptions.setHostnameVerificationAlgorithm("")
        } else {
            pgConnectOptions.setHostnameVerificationAlgorithm(algo)
        }
    }

    private fun configureReconnection(
        pgConnectOptions: PgConnectOptions,
        dataSourceReactiveRuntimeConfig: DataSourceReactiveRuntimeConfig,
    ) {
        pgConnectOptions.setReconnectAttempts(dataSourceReactiveRuntimeConfig.reconnectAttempts())

        pgConnectOptions.setReconnectInterval(
            dataSourceReactiveRuntimeConfig.reconnectInterval().toMillis()
        )
    }

    private fun configureMetrics(pgConnectOptions: PgConnectOptions, dataSourceName: String) {
        // Use the convention defined by Quarkus Micrometer Vert.x metrics to create
        // metrics prefixed with postgresql and the client_name as tag.
        pgConnectOptions.setMetricsName("postgresql|$dataSourceName")
    }
}
