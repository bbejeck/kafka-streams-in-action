package bbejeck.util.collection;


public class Tuple<L, R> {

    public final L _1;
    public final R _2;

    public Tuple(L _1, R _2) {
        this._1 = _1;
        this._2 = _2;
    }

  public static <L, R> Tuple<L, R> of (L left, R right) {
        return new Tuple<>(left, right);

  }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tuple)) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (_1 != null ? !_1.equals(tuple._1) : tuple._1 != null) return false;
        return _2 != null ? _2.equals(tuple._2) : tuple._2 == null;
    }

    @Override
    public int hashCode() {
        int result = _1 != null ? _1.hashCode() : 0;
        result = 31 * result + (_2 != null ? _2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Tuple{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
}
