package cn.com.microintelligence.config;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigService;
import org.apache.commons.lang3.StringUtils;

/**
 * @author lucas
 */
public class ApolloConfigManager implements java.io.Serializable {

    private static final ApolloConfigManager AM = new ApolloConfigManager();

    private Config config = null;

    public static ApolloConfigManager getInstance() {
        return AM;
    }

    public void loadNamespace(String namespace) {
        if (AM.config == null) {
            synchronized (ApolloConfigManager.class) {
                if (StringUtils.isNotBlank(namespace)) {
                    AM.config = ConfigService.getConfig(namespace);
                } else {
                    throw new IllegalArgumentException("apollo namespace is empty");
                }
            }
        }
    }

    public String getString(String key) {
        return get(key).toString();
    }

    public Integer getInt(String key) {
        return Integer.valueOf(getString(key));
    }

    public long getLong(String key) {
        return Long.parseLong(getString(key));
    }

    /**
     * read from apollo config
     */
    public Object get(String key) {
        if (AM.config == null) {
            throw new IllegalStateException("can not init Apollo config : " + key);
        }
        Object any = AM.config.getProperty(key, null);
        if (any == null) {
            throw new IllegalArgumentException("can not found conf key " + key);
        }
        return any;
    }

}
