//package com.kasikornbank.dih.hsm;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;

public class Util {

	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

	public static byte[] hexToBytes(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	public static String bytesToHex(byte[] bytes) {
		char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}

	public static String maskCardNumber(String cardNumber, String maskPattern) {
		return Util.maskCardNumber(cardNumber, maskPattern, '*');
	}

	public static String maskCardNumber(String cardNumber, String maskPattern, char maskChar) {

		// format the number
		int index = 0;
		StringBuilder maskedNumber = new StringBuilder();
		for (int i = 0; i < maskPattern.length(); i++) {
			char c = maskPattern.charAt(i);
			if (c == '%') {
				maskedNumber.append(cardNumber.charAt(index));
				index++;
			} else if (c == maskChar) {
				maskedNumber.append(c);
				index++;
			} else {
				maskedNumber.append(c);
			}
		}

		// return the masked number
		return maskedNumber.toString();
	}

	public static String padLeftZeros(String str, int n) {
		return String.format("%1$" + n + "s", str).replace(' ', '0');
	}

	public static String decryptAES(AES aesKey, String massage) throws BadPaddingException, IllegalBlockSizeException {
        String decryptMassage = aesKey.decryptToString(massage);
        return decryptMassage;
    }
}
