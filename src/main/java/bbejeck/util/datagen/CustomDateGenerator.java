package bbejeck.util.datagen;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Generates dates either with uniform increases in time
 * or within random periods of time with increases and occasional
 * late arrival data
 */
public class CustomDateGenerator {

    private Instant instant = Instant.now();
    private Duration increaseDuration;

    private CustomDateGenerator(Duration increaseDuration) {
        this.increaseDuration = increaseDuration;
    }

    public static CustomDateGenerator withTimestampsIncreasingBy(Duration increaseDuration) {
           return new CustomDateGenerator(increaseDuration);
    }

    public Date get() {
          Date date =  Date.from(instant);
          instant = instant.plus(increaseDuration);
          return date;
    }
}
