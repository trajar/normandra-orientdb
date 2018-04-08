package org.normandra.orientdb.data;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.HttpHostConnectException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * a local server that is spawned locally and used as embedded service
 *
 * @date 2/15/18.
 */
public class LocalEmbeddedServer {
    private static final Logger logger = LoggerFactory.getLogger(LocalEmbeddedServer.class);

    private final File orientDir;

    private final String serverUser;

    private final String serverPwd;

    private int binaryPort = -1;

    private int httpPort = -1;

    private Process activeProcess = null;

    private Thread shutdownHook = null;

    public LocalEmbeddedServer(final File orientDir) {
        this(orientDir, "root", rootPassword());
    }

    public LocalEmbeddedServer(final File orientDir, final String serverUser, final String serverPwd) {
        this.orientDir = orientDir;
        this.serverUser = serverUser;
        this.serverPwd = serverPwd;
    }

    public boolean startIfNotRunning() throws Exception {
        if (this.isRunning()) {
            // already running
            return true;
        }

        // spawn process
        final File bin = new File(this.orientDir, "bin").getCanonicalFile();
        if (!bin.exists()) {
            throw new IllegalStateException();
        }
        final String rootPwd = rootPassword();
        ProcessBuilder startup = new ProcessBuilder();
        if (SystemUtils.IS_OS_WINDOWS) {
            startup.command(new File(bin, "server.bat").getCanonicalPath());
        } else {
            startup.command("sh", new File(bin, "server.sh").getCanonicalPath());
        }
        if ("root".equalsIgnoreCase(this.serverUser) && rootPwd != null && !rootPwd.isEmpty()) {
            logger.trace("Setting local server root password.");
            startup.environment().put("ORIENTDB_ROOT_PASSWORD", rootPwd);
            startup.environment().put("JAVA_OPTS", "-DORIENTDB_ROOT_PASSWORD=" + rootPwd);
        }
        this.activeProcess = startup.start();

        // register shutdown
        this.shutdownHook = new Thread(() -> {
            try {
                ProcessBuilder shutdown = new ProcessBuilder();
                if (SystemUtils.IS_OS_WINDOWS) {
                    shutdown.command(new File(bin, "shutdown.bat").getCanonicalPath());
                } else {
                    shutdown.command("sh", new File(bin, "shutdown.sh").getCanonicalPath());
                }
                shutdown.start();
            } catch (final Exception e) {
                logger.warn("Unable to execute shutdown script.", e);
            }
        });
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
        return true;
    }

    public boolean waitUntilReady() throws InterruptedException {
        return this.waitUntilReady(-1);
    }

    public boolean waitUntilReady(final long timeoutMsec) throws InterruptedException {
        final long startTime = System.currentTimeMillis();
        while (timeoutMsec <= 0 || System.currentTimeMillis() - startTime <= timeoutMsec) {
            if (this.isRunning()) {
                logger.debug("Server status confirmed as running.");
                return true;
            }
            final long sleepMsec = timeoutMsec > 0 ? Math.min(500, timeoutMsec) : 250;
            Thread.sleep(sleepMsec);
        }
        logger.warn("Unable to determine server running status.");
        return false;
    }

    public void stop() throws InterruptedException {
        this.stop(-1);
    }

    public void stop(final long waitMs) throws InterruptedException {
        if (this.shutdownHook != null) {
            // shutdown server via hook
            Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
            this.shutdownHook.start();
            if (waitMs > 0) {
                this.shutdownHook.join(waitMs);
            } else {
                this.shutdownHook.join();
            }
            this.shutdownHook = null;
        }

        if (this.activeProcess != null) {
            if (this.activeProcess.isAlive()) {
                // something probably went very wrong, but ensure process is stopped
                if (this.isRunning()) {
                    this.activeProcess.destroy();
                }
            }
            this.activeProcess = null;
        }
    }

    public boolean isRunning() {
        try {
            final String http = this.getHttpUrl() + "/server";
            final Map<String, ?> status = this.executeJsonServerRequest(new HttpGet(http));
            if (null == status || status.isEmpty()) {
                logger.warn("Found empty status, server not likely ready.");
                return false;
            }
            final Object connectionsObj = status.get("connections");
            logger.debug("Found connection status value of " + connectionsObj + ".");
            if (connectionsObj instanceof Collection) {
                return ((Collection) connectionsObj).size() > 0;
            }
        } catch (final HttpHostConnectException e) {
            logger.trace("Unable to connect to local server.", e);
        } catch (final Exception e) {
            logger.warn("Unable to check local server status.", e);
        }
        return false;
    }

    public Map<String, ?> getDatabase(final String dbname) throws IOException {
        final String http = this.getHttpUrl() + "/database/" + dbname;
        return this.executeJsonServerRequest(new HttpGet(http));
    }

    public boolean createDatabase(final String dbname) throws IOException {
        return this.createDatabase(dbname, "plocal");
    }

    public boolean createDatabase(final String dbname, final String type) throws IOException {
        if (null == dbname || dbname.isEmpty()) {
            return false;
        }
        final String http = this.getHttpUrl() + "/database/" + dbname + "/" + type;
        final Map<String, ?> response = this.executeJsonServerRequest(new HttpPost(http));
        if (null == response || response.isEmpty()) {
            return false;
        }
        return true;
    }

    public String getBinaryUrl() {
        return "remote:localhost";
    }

    public String getHttpUrl() {
        return "http://localhost:" + this.getHttpPort();
    }

    private Map<String, ?> executeJsonServerRequest(final HttpUriRequest request) throws IOException {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final byte[] creds = (this.serverUser + ":" + this.serverPwd).getBytes(Charset.forName("UTF-8"));
        request.setHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(creds));
        request.setHeader("Accept-Encoding", "gzip,deflate");
        request.setHeader("Accept", "text/html,application/json");
        try {
            final CloseableHttpResponse response = httpclient.execute(request);
            if (response.getStatusLine().getStatusCode() == 401) {
                throw new IllegalStateException("Unable to query server, received 401 status from [" + this.getHttpUrl() + "].");
            }
            try {
                final HttpEntity entity = response.getEntity();
                final ObjectMapper mapper = new ObjectMapper();
                final Map map = mapper.readValue(entity.getContent(), Map.class);
                EntityUtils.consume(entity);
                return map;
            } finally {
                response.close();
            }
        } finally {
            httpclient.close();
        }
    }

    public int getBinaryPort() {
        if (this.binaryPort > 0) {
            return this.binaryPort;
        }
        this.binaryPort = this.findNetworkPortByType("binary");
        if (this.binaryPort <= 0) {
            final int defaultPort = 2424;
            logger.warn("Unable to determine port number for local server, defaulting to " + defaultPort + ".");
            this.binaryPort = defaultPort;
        }
        return this.binaryPort;
    }

    public int getHttpPort() {
        if (this.httpPort > 0) {
            return this.httpPort;
        }
        this.httpPort = this.findNetworkPortByType("http");
        if (this.httpPort <= 0) {
            final int defaultPort = 2480;
            logger.warn("Unable to determine port number for local server, defaulting to " + defaultPort + ".");
            this.httpPort = defaultPort;
        }
        return this.httpPort;
    }

    private int findNetworkPortByType(final String type) {
        try {
            final ServerConfiguration config = this.readConfig();
            if (null == config || config.isEmpty()) {
                throw new IOException("Configuration file is empty.");
            }
            return config.getNetwork().findPort(type);
        } catch (final Exception e) {
            logger.warn("Unable to read server configuration.", e);
        }
        return -1;
    }

    private ServerConfiguration readConfig() throws IOException {
        final ObjectMapper mapper = new XmlMapper();
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
        mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
        mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
        mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.ANY)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE));
        return mapper.readValue(findConfig(), ServerConfiguration.class);
    }

    private File findConfig() throws IOException {
        // check installation
        if (!this.orientDir.exists()) {
            throw new FileNotFoundException("Unable to locate path.");
        }
        if (!this.orientDir.isDirectory()) {
            throw new IOException("Path is file not directory.");
        }
        // check configuration
        final File configDir = new File(this.orientDir, "config");
        if (!configDir.exists() || !configDir.isDirectory()) {
            throw new IOException("Unable to locate configuration folder.");
        }
        return new File(configDir, "orientdb-server-config.xml");
    }

    public static class ServerConfiguration {
        @JsonProperty
        private NetworkConfiguration network;

        public NetworkConfiguration getNetwork() {
            if (null == this.network) {
                return new NetworkConfiguration();
            } else {
                return this.network;
            }
        }

        public boolean isEmpty() {
            if (null == this.network || this.network.isEmpty()) {
                return true;
            }
            return false;
        }
    }

    public static class NetworkConfiguration {
        @JsonProperty
        private List<Map> sockets;

        @JsonProperty
        private List<Map> protocols;

        @JsonProperty
        private List<Map> listeners;

        public int findPort(final String protocolType) {
            if (null == protocolType) {
                return -1;
            }
            if (null == this.listeners || this.listeners.isEmpty()) {
                return -1;
            }
            for (final Map map : this.listeners) {
                final Object protocol = map.get("protocol");
                if (protocol != null && protocolType.equalsIgnoreCase(protocol.toString())) {
                    // check port number
                    final Object port = map.get("port");
                    if (port != null) {
                        return Integer.parseInt(port.toString());
                    }
                    // check port range
                    final Object range = map.get("port-range");
                    if (range != null) {
                        final String[] parts = range.toString().split("-");
                        if (parts != null && parts.length == 2) {
                            return Integer.parseInt(parts[0]);
                        }
                    }
                }
            }
            return -1;
        }

        public boolean isEmpty() {
            if (this.sockets != null && !this.sockets.isEmpty()) {
                return false;
            }
            if (this.protocols != null && !this.protocols.isEmpty()) {
                return false;
            }
            if (this.listeners != null && !this.listeners.isEmpty()) {
                return false;
            }
            return true;
        }
    }

    private static String rootPassword() {
        // check for default root password for new/clean installations
        final String envPwd = System.getenv("ORIENTDB_ROOT_PASSWORD");
        if (envPwd != null && !envPwd.isEmpty()) {
            return envPwd;
        }
        return System.getProperty("ORIENTDB_ROOT_PASSWORD");
    }
}
