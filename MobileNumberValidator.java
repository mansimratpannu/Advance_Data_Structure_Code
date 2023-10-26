import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class MobileNumberValidator {
    private static Map<String, String> countryRegexMap;

    static {
        countryRegexMap = new HashMap<>();
        // Add country codes and regular expressions for mobile numbers here
        countryRegexMap.put("US", "^(\\+1|1)?[2-9][0-9]{9}$");
        countryRegexMap.put("UK", "^(\\+44|0)[7-9][0-9]{9}$");
        // Add more countries and their regular expressions as needed
    }

    public static boolean isValidMobileNumber(String countryCode, String phoneNumber) {
        String regex = countryRegexMap.get(countryCode);
        if (regex == null) {
            System.out.println("Country code not found.");
            return false;
        }

        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(phoneNumber);

        return matcher.matches();
    }

    public static void main(String[] args) {
        // Test cases
        String countryCode = "US";
        String phoneNumber = "+14155552671";
        boolean isValid = isValidMobileNumber(countryCode, phoneNumber);
        if (isValid) {
            System.out.println("Valid mobile number for " + countryCode);
        } else {
            System.out.println("Invalid mobile number for " + countryCode);
        }

        countryCode = "UK";
        phoneNumber = "07555123456";
        isValid = isValidMobileNumber(countryCode, phoneNumber);
        if (isValid) {
            System.out.println("Valid mobile number for " + countryCode);
        } else {
            System.out.println("Invalid mobile number for " + countryCode);
        }
    }
}
