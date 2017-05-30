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
    public String toString() {
        return "Tuple{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
}
