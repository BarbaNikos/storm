package gr.katsip.spear;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
public class ApproxConfiguration {
  
  public static int getBValue(final float epsilon, final long N) {
    int error = (int) (epsilon * 100.0f);
    if (error == 10) {
      if (N <= 1e+5) return 5;
      if (N <= 1e+6) return 7;
      if (N <= 1e+7) return 10;
      if (N <= 1e+8) return 15;
      return 12;
    } else if (error == 5) {
      if (N <= 1e+6) return 6;
      if (N <= 1e+7) return 8;
      if (N <= 1e+8) return 7;
      return 8;
    }else if (error == 1) {
      if (N <= 1e+5) return 7;
      if (N <= 1e+6) return 12;
      if (N <= 1e+7) return 9;
      return 10;
    } else if (epsilon <= 0.005f && epsilon > 0.001f) {
      if (N <= 1e+5) return 3;
      if (N <= 1e+8) return 8;
      return 7;
    } else if (epsilon <= 0.001f && epsilon > 0.0000f) {
      if (N <= 1e+5) return 3;
      if (N <= 1e+6) return 5;
      if (N <= 1e+7) return 5;
      if (N <= 1e+8) return 9;
      return 10;
    }
    throw new IllegalArgumentException("support for error values: {10, 5, 1, 0.05, 0.01}%");
  }
  
  public static int getSampleBValue(final int epsilon, final int confidence) {
    int delta = 100 - confidence;
    if (epsilon == 10) {
      if (delta == 1) return 3;
      return 5;
//      if (delta == 0.001f) return 4;
//      if (delta == 0.0001f) return 5;
    } else if (epsilon == 5) {
      if (delta == 1) return 4;
      return 5;
    } else if (epsilon == 1) {
      return 6;
    } else {
      return 7;
    }
// else if (epsilon <= 0.005f && epsilon > 0.001f) {
//      if (delta == 0.01f) return 6;
//      return 7;
//    } else if (epsilon <= 0.001f && epsilon > 0.0000f) {
//      if (delta == 0.01f) return 8;
//      if (delta == 0.001f) return 8;
//      return 9;
//    }
//    throw new IllegalArgumentException("support for error values: {10, 5, 1, 0.05, 0.01}%");
  }
  
  public static int getKValue(final float epsilon, final long N) {
    int error = (int) (epsilon * 100.0f);
    if (error == 10) {
      if (N <= 1e+5) return 55;
      if (N <= 1e+6) return 54;
      if (N <= 1e+7) return 60;
      if (N <= 1e+8) return 51;
      return 77;
    } else if (error == 5) {
      if (N <= 1e+5) return 78;
      if (N <= 1e+6) return 117;
      if (N <= 1e+7) return 129;
      if (N <= 1e+8) return 211;
      return 235;
    }else if (error == 1) {
      if (N <= 1e+5) return 217;
      if (N <= 1e+6) return 229;
      if (N <= 1e+7) return 412;
      if (N <= 1e+8) return 596;
      return 765;
    } else if (epsilon <= 0.005f && epsilon > 0.001f) {
      if (N <= 1e+5) return 953;
      if (N <= 1e+6) return 583;
      if (N <= 1e+7) return 875;
      if (N <= 1e+8) return 1290;
      return 2106;
    } else if (epsilon <= 0.001f && epsilon > 0.0000f) {
      if (N <= 1e+5) return 2778;
      if (N <= 1e+6) return 3031;
      if (N <= 1e+7) return 5495;
      if (N <= 1e+8) return 4114;
      return 5954;
    }
    throw new IllegalArgumentException("support for error values: {10, 5, 1, 0.05, 0.01}%");
  }
  
  public static int getSampleKValue(final int epsilon, final int confidence) {
//    int error = (int) (epsilon * 100.0f);
    int delta = 100 - confidence;
    if (epsilon == 10) {
      if (delta == 1) return 42;
      return 36;
//      if (delta == 0.001f) return 36;
//      if (delta == 0.0001f) return 31;
    } else if (epsilon == 5) {
      if (delta == 1) return 79;
      return 76;
//      if (delta == 0.001f) return 71;
//      if (delta == 0.0001f) return 76;
    } else if (epsilon == 1) {
      if (delta == 1) return 408;
      return 472;
//      if (delta == 0.001f) return 447;
//      if (delta == 0.0001f) return 472;
    } else {
      return 937;
    }
//    else if (epsilon <= 0.005f && epsilon > 0.001f) {
//      if (delta == 0.01f) return 962;
//      if (delta == 0.001f) return 893;
//      if (delta == 0.0001f) return 937;
//    } else if (epsilon <= 0.001f && epsilon > 0.0000f) {
//      if (delta == 0.01f) return 4964;
//      if (delta == 0.001f) return 5326;
//      if (delta == 0.0001f) return 4943;
//    }
//    throw new IllegalArgumentException("support for error values: {10, 5, 1, 0.05, 0.01}%");
  }
}
