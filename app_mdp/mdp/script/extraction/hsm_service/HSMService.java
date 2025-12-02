//package com.kasikornbank.dih.hsm;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Locale;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

/**
 * The <code>HSMService</code> object provides ready-made methods to communicate
 * with HSM server
 *
 * <p>
 * Use this service to obtain decrypted AES-128 key from HSM and use
 * <code>AES</code> with the key to decrypt card number from CCMS
 *
 * @author ronnawit.t
 * @see tech.kbtg.kline.AES
 */
public class HSMService {

    private final String header = "01010000003F";
    private final String encAesFn = "EE0808";
    private final String decAesFn = "EE0809";
    private final String fm = "00";
    private final String dpk;
    private final String cm = "00";
    private final String icv = "00000000000000000000000000000000";
    private final String host;
    private final int port;
    private int timeout = 10000;
    private int bufferSize = 1024;

    /**
     * Construct an instance to hold specified HSM properties
     *
     * @param host the host name address of HSM server, or null for the loopback
     *             address.
     * @param port the port number
     * @param dpk  the key specifier for DPK (Formats: 52, 53, 1C) provided by HSM
     * @throws IllegalArgumentException if the port parameter is outside the specified range of valid port
     *                                  values, which is between 0 and 65535.
     * @throws NullPointerException     if the specified dpk is null
     */
    public HSMService(String host, int port, String dpk) {
        if (port < 0 && port > 65535) {
            throw new IllegalArgumentException();
        }
        if (dpk == null) {
            throw new NullPointerException();
        }
        this.host = host;
        this.port = port;
        this.dpk = dpk;
    }

    /**
     * @param key a key to encrypt
     * @return the encrypted key in hexadecimal format, or <code>null</code> if got
     * invalid reply from HSM server
     * @throws UnknownHostException   if the IP address of the host could not be determined.
     * @throws IOException            if an I/O error occurs when creating a socket to HSM server, or
     *                                socket is timed out.
     * @throws SocketTimeoutException if cannot get reply from HSM before timeout
     */
    public String encryptToHex(byte[] key) throws UnknownHostException, IOException {
        if (key == null) {
            throw new NullPointerException();
        }
        String len = String.format("%02x", key.length).toUpperCase(Locale.ENGLISH);
        String req = header + encAesFn + fm + dpk + cm + icv + len + Util.bytesToHex(key);
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            socket.getOutputStream().write(Util.hexToBytes(req));
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 27) {
                int dataLen = b[26];
                return Util.bytesToHex(Arrays.copyOfRange(b, 27, 27 + dataLen));
            }
            return null;
        }
    }


    /**
     * @param key a key to encrypt in hexadecimal format
     * @return the encrypted key in hexadecimal format, or <code>null</code> if got
     * invalid reply from HSM server
     * @throws UnknownHostException   if the IP address of the host could not be determined.
     * @throws IOException            if an I/O error occurs when creating a socket to HSM server.
     * @throws SocketTimeoutException if cannot get reply from HSM before timeout
     */
    public String encryptToHex(String key) throws UnknownHostException, IOException, NullPointerException {
        if (key == null) {
            throw new NullPointerException();
        }
        String len = String.format("%02x", key.length() / 2).toUpperCase(Locale.ENGLISH);
        String req = header + encAesFn + fm + dpk + cm + icv + len + key;
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            socket.getOutputStream().write(Util.hexToBytes(req));
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 27) {
                int dataLen = b[26];
                return Util.bytesToHex(Arrays.copyOfRange(b, 27, 27 + dataLen));
            }
            return null;
        }
    }

    /**
     * @param key a key to decrypt in hexadecimal format
     * @return the encrypted key in hexadecimal format, or <code>null</code> if got
     * invalid reply from HSM server
     * @throws UnknownHostException   if the IP address of the host could not be determined.
     * @throws IOException            if an I/O error occurs when creating a socket to HSM server.
     * @throws SocketTimeoutException if cannot get reply from HSM before timeout
     */
    public String decryptToHex(String hexData) throws UnknownHostException, IOException {
        if (hexData == null) {
            throw new NullPointerException();
        }
        String len = String.format("%02x", hexData.length() / 2).toUpperCase(Locale.ENGLISH);
        String req = header + decAesFn + fm + dpk + cm + icv + len + hexData;
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            socket.getOutputStream().write(Util.hexToBytes(req));
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 27) {
                int dataLen = b[26];
                return Util.bytesToHex(Arrays.copyOfRange(b, 27, 27 + dataLen));
            }
            return null;
        }
    }

    /**
     * @param key a key to decrypt in hexadecimal format
     * @return the encrypted key byte array, or <code>null</code> if got invalid
     * reply from HSM server
     * @throws UnknownHostException if the IP address of the host could not be determined.
     * @throws IOException          if an I/O error occurs when creating a socket to HSM server.
     */
    public byte[] decryptToBytes(String hexData) throws UnknownHostException, IOException {
        if (hexData == null) {
            throw new NullPointerException();
        }
        String len = String.format("%02x", hexData.length() / 2).toUpperCase(Locale.ENGLISH);
        String req = header + decAesFn + fm + dpk + cm + icv + len + hexData;
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            socket.getOutputStream().write(Util.hexToBytes(req));
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 27) {
                int dataLen = b[26];
                return Arrays.copyOfRange(b, 27, 27 + dataLen);
            }
            return new byte[0];
        }
    }

    /**
     * Provides interface to use other available function of HSM
     *
     * @param request the request
     * @return reply from HSM
     * @throws UnknownHostException if the IP address of the host could not be determined.
     * @throws IOException          if an I/O error occurs when creating a socket to HSM server.
     */
    public byte[] customFunction(byte[] request) throws UnknownHostException, IOException {
        if (request == null) {
            throw new NullPointerException();
        }
        try (Socket socket = new Socket(host, port)) {
            socket.setSoTimeout(timeout);
            socket.getOutputStream().write(request);
            InputStream is = socket.getInputStream();
            byte[] b = new byte[bufferSize];
            if (is.read(b) >= 0) {
                return b;
            }
            return new byte[0];
        }
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public static void main(String[] args) throws UnknownHostException, IOException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException
    {
        // Check if the required argument is provided
        if (args.length < 4) {
            System.out.println("Please provide the encrypted message as an argument.");
            return;
        }

        // Get the encrypted message from the command-line argument
        String encryptedMsg = args[0];
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String dpk = args[3];

        HSMService service = new HSMService(host, port, dpk);
        String key = service.decryptToHex(encryptedMsg);

        // Print the AES key to the console
        System.out.println(key);
    }
}
