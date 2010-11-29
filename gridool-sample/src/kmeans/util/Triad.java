package kmeans.util;

import java.io.Serializable;

public class Triad<T1, T2, T3> implements Serializable {

    private static final long serialVersionUID = -1125789505944625007L;

    public T1 first;
    public T2 second;
    public T3 third;

    public Triad(T1 first, T2 second, T3 third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

}
