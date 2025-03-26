package cs6650Spring2025.util;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class to read configuration properties from config.properties file
 */
public class ConfigReader {

  private static final Logger logger = Logger.getLogger(ConfigReader.class.getName());
  private static Properties properties = new Properties();
  private static final String CONFIG_FILE = "config.properties";
  private static boolean loaded = false;

  /**
   * Load properties from the configuration file
   */
  public static void loadProperties() {
    if (!loaded) {
      try (InputStream input = new FileInputStream(CONFIG_FILE)) {
        properties.load(input);
        loaded = true;
        logger.info("Successfully loaded configuration from " + CONFIG_FILE);
      } catch (IOException ex) {
        logger.log(Level.SEVERE, "Failed to load configuration file", ex);
        // Try loading from classpath
        try (InputStream resourceStream = ConfigReader.class.getClassLoader()
            .getResourceAsStream(CONFIG_FILE)) {
          if (resourceStream != null) {
            properties.load(resourceStream);
            loaded = true;
            logger.info("Successfully loaded configuration from classpath");
          } else {
            logger.severe("Configuration file not found in classpath");
          }
        } catch (IOException e) {
          logger.log(Level.SEVERE, "Failed to load configuration from classpath", e);
        }
      }
    }
  }

  /**
   * Get a property as a string
   *
   * @param key          the property key
   * @param defaultValue default value if property not found
   * @return the property value or default value
   */
  public static String getProperty(String key, String defaultValue) {
    if (!loaded) {
      loadProperties();
    }
    return properties.getProperty(key, defaultValue);
  }

  /**
   * Get a property as an integer
   *
   * @param key          the property key
   * @param defaultValue default value if property not found
   * @return the property value as integer or default value
   */
  public static int getIntProperty(String key, int defaultValue) {
    String value = getProperty(key, String.valueOf(defaultValue));
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      logger.log(Level.WARNING, "Failed to parse integer property: " + key, e);
      return defaultValue;
    }
  }
}