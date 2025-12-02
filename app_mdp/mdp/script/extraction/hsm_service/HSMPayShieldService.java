//package com.kasikornbank.dih.hsm;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HSMPayShieldService {

    private final String header = "000000000000000000000000000000";//Fix
    private final String commandCodeString = "M2";//Fix
    private final String modFlagString = "00";//Fix
    private final String inFormaString = "1";//Fix
    private final String outFormaString = "1";//Fix
    private final String keyTypString = "FFF";//Fix
    private final String keyHeadeString = "S1009621AN00S0001";//Fix
    //private String keyEncryptedString = "3ADA78F286B6E4487D1F2CC1882C96648F7B4033DF055F46ED7A50EB0F5A092E1D380038E49520BA";//DPK2
    private String keyEncryptedString = "0BB88077EEA622C49F58943DEA820EB03B241F132340735846CE8DCD156C51CB55AFB72BA6F4BD8A";
    private final String msgLengthString = "0020";//Fix
    private String lmkString = "%01";

    private String host;
    private int port;
    private int timeout = 10000;
    private int bufferSize = 1024;
    private String commandPrefixString = "00";

    public HSMPayShieldService(String host, int port, int timeout, String keyEncryptedString, String lmkString) throws IOException {
        if (port < 0 && port > 15000) {//1500
            throw new IllegalArgumentException();
        }
        if (keyEncryptedString == null) {
            throw new NullPointerException();
        }
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.keyEncryptedString = keyEncryptedString;
        this.lmkString = lmkString;
    }

    public boolean checkHostAvailability() throws UnknownHostException, SocketTimeoutException, IOException {
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout); return true;
        }catch(IOException ie){
            return false;
        }
    }

    public String decryptToHex(String encryptedString) throws UnknownHostException, IOException, SocketTimeoutException {
        if (encryptedString == null) {
            throw new NullPointerException();
        }

        //Server command
        String commandString = header
            + commandCodeString
            + modFlagString
            + inFormaString
            + outFormaString
            + keyTypString
            + keyHeadeString
            + keyEncryptedString
            + msgLengthString
            + encryptedString
            + lmkString;

        //Convert String to Hex
        String hexString = Util.bytesToHex(commandString.getBytes());

        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);

            //Calculate length/2 and convert DecToHex and adjust to 2 byte
            if(null!=hexString && hexString.length()>0){
                String lengthDecTohex = Integer.toHexString(hexString.length()/2);
                //commandPrefixString = Integer.toHexString(commandPrefixString.length()/2);
                commandPrefixString += lengthDecTohex;
            }

            //commandPrefixString = 00ac, 00af
            //System.out.println("SEND>>" + commandPrefixString + hexString);
            socket.getOutputStream().write(Util.hexToBytes(commandPrefixString + hexString));
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];

            if (is.read(b) >= 0) {
                return Util.bytesToHex(b);
            }
            return null;
        }
    }

    public byte[] customFunction(byte[] request) throws UnknownHostException, IOException {
        if (request == null) {
            throw new NullPointerException();
        }
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            System.out.println("SEND>>"+request);
            socket.getOutputStream().write(request);
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 0) {
                return b;
            }
            return new byte[0];
        }
    }

    public static void main(String[] args) throws Exception {
        //System.out.println("HSM Payshield Test App.......Start");

        //HSM Test environment : 172.30.153.34 port 2020
        //Local tunnel : ssh -L 12020:172.30.153.34:2020 user@remote-server
        if (args.length < 4) {
            System.out.println("Please provide the encrypted message as an argument.");
            return;
        }

        // Get the encrypted message from the command-line argument
        String encryptedMsg = args[0].toUpperCase();
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String dpk = args[3];

        // // Example of how you might use the arguments
        // System.out.println("Encrypted Message: " + encryptedMsg);
        // System.out.println("Host: " + host);
        // System.out.println("Port: " + port);
        // System.out.println("DPK: " + dpk);

        //Key
        HSMPayShieldService service = new HSMPayShieldService(host, port, 60000, dpk, "%01");
        String key = new String(Util.hexToBytes(service.decryptToHex(encryptedMsg)), StandardCharsets.UTF_8) ;
        key = key.substring(40, 72);
        System.out.println(key);
    }
}
