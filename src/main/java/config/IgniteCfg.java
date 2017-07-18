package config;

import org.apache.ignite.IgniteSpringBean;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * @author tonycox
 * @since 18.07.17
 */
@Configuration
public class IgniteCfg {

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public String gridName() {
        return UUID.randomUUID().toString();
    }

    @Bean
    public String localHost() {
        return "127.0.0.1";
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public DiscoverySpi discoverySpi() {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        spi.setIpFinder(ipFinder());
        return spi;
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public TcpDiscoveryIpFinder ipFinder() {
        TcpDiscoveryVmIpFinder tdVmIp = new TcpDiscoveryVmIpFinder();
        tdVmIp.setAddresses(discoverIps());
        return tdVmIp;
    }

    @Bean
    public List<String> discoverIps() {
        return Collections.singletonList(localHost() + ":47500..47509");
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public IgniteConfiguration igniteConfiguration() {
        return new IgniteConfiguration() {{
            setCacheConfiguration();
            setLocalHost(localHost());
            setGridName(gridName());
            setDiscoverySpi(discoverySpi());
        }};
    }

    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public IgniteSpringBean igniteBean() {
        IgniteSpringBean ignite = new IgniteSpringBean();
        ignite.setConfiguration(igniteConfiguration());
        return ignite;
    }
}
