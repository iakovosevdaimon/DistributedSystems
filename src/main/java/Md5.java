import javax.xml.bind.DatatypeConverter;
import java.math.BigInteger;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5 {

    //private MessageDigest md;
    public Md5(){}

    public static String calculateHash(String toHash){
        String hash = null;
        BigInteger no = null;
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(toHash.getBytes());
            byte[] digest = md.digest();
            hash = DatatypeConverter.printHexBinary(digest).toUpperCase();
            //byte[] digest = md.digest(toHash.getBytes());
            // Convert byte array into signum representation
            no = new BigInteger(1, digest);

            System.out.println(String.valueOf(no));
            BigInteger res = no.mod(new BigInteger("172417846330272637300962415361370079674"));
            System.out.println(res);

            // Convert message digest into hex value
            /*
            hash = no.toString(16);
            //System.out.println(hash);
            while (hash.length() < 32) {
                hash = "0" + hash;
            }
            hash = hash.toUpperCase();
            System.out.println(hash);
            */

        } catch (NoSuchAlgorithmException e) {
            System.out.println("Exception to calculate hash");
            e.printStackTrace();
        }
        return hash;
    }

    public static String compareHashes(String h1, String h2){
        int c = h1.compareTo(h2);
        switch(c) {
            case 1:
                return h1;

            case -1:
                return h2;

            default:
                return h1;
        }
    }

    public static BigInteger compareHashes(BigInteger n1, BigInteger n2){
        int c = n1.compareTo(n2);
        switch(c) {
            case 1:
                return n1;

            case -1:
                return n2;

            default:
                return n1;
        }
    }

    public static BigInteger modulo(BigInteger n1, BigInteger n2){
        //always n2 = minumum hash of brokers
        return n1.mod(n2);
    }

    public static String modulo(String h1, String h2){
        //always h2 = minumum hash of brokers
        BigInteger n1 = new BigInteger(h1,16);
        BigInteger n2 = new BigInteger(h2,16);
        BigInteger result = n1.mod(n2);
        String hash = result.toString(16);
        //System.out.println(hash);
        while (hash.length() < 32) {
            hash = "0" + hash;
        }
        hash = hash.toUpperCase();
        return hash;
    }

    public static void main(String args[]){
        String h1 = "192.168.1.15+550";
        System.out.println(h1);
        String ha1 = calculateHash(h1);
        System.out.println(ha1);
        String h2 = "Cavin Louis";
        System.out.println(h2);
        String ha2 = calculateHash(h2);
        System.out.println(new BigInteger(ha2,16));
        System.out.println(ha2);
        System.out.println(compareHashes(ha1,ha2));
    }
}
