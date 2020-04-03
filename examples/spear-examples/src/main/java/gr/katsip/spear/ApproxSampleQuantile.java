package gr.katsip.spear;

/**
 * @author Nikos R. Katsipoulakis (nick.katsip@gmail.com)
 */
public class ApproxSampleQuantile extends ApproxQuantile {
  
  public ApproxSampleQuantile(int epsilon, int confidence) {
    super(ApproxConfiguration.getSampleBValue(epsilon, confidence),
        ApproxConfiguration.getSampleKValue(epsilon, confidence));
  }
}
