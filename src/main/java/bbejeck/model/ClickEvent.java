package bbejeck.model;

import java.time.Instant;


public class ClickEvent {

    private String symbol;
    private String link;
    private Instant timestamp;

    public ClickEvent(String symbol, String link, Instant timestamp) {
        this.symbol = symbol;
        this.link = link;
        this.timestamp = timestamp;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getLink() {
        return link;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "ClickEvent{" +
                "symbol='" + symbol + '\'' +
                ", link='" + link + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClickEvent)) return false;

        ClickEvent that = (ClickEvent) o;

        if (symbol != null ? !symbol.equals(that.symbol) : that.symbol != null) return false;
        if (link != null ? !link.equals(that.link) : that.link != null) return false;
        return timestamp != null ? timestamp.equals(that.timestamp) : that.timestamp == null;
    }

    @Override
    public int hashCode() {
        int result = symbol != null ? symbol.hashCode() : 0;
        result = 31 * result + (link != null ? link.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        return result;
    }
}
