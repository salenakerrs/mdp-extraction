//package com.kasikornbank.dih.hsm;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * This class provides methods for encryption and decryption with algorithm AES
 * (128-bit) and cipher mode ECB without padding (<i>"AES/ECB/NoPadding"</i>).
 *
 * <p>
 * Implement this class after obtain decrypted key from HSM and pass the key to
 * construct. For example:
 *
 * <pre>
 * HSMService hsm = ...;
 * AES aes = new AES(hsm.decryptKeyToHex(...));
 * ...
 * </pre>
 *
 * <p>
 * Please note that CCMS is using Go language, strings are UTF-8 by default.
 *
 * @see tech.kbtg.kline.HSMService
 * @author ronnawit.t
 *
 */
public class AES {

	private Cipher decryptCipher;
	private Cipher encryptCipher;

	/**
	 * Create object with initialized cryptographic cipher
	 *
	 * @param key
	 *          the key
	 * @throws InvalidKeyException
	 *           if the given key is inappropriate for initializing this cipher, or
	 *           requires algorithm parameters that cannot be determined from the
	 *           given key, or if the given key has a keysize that exceeds the
	 *           maximum allowable keysize (as determined from the configured
	 *           jurisdiction policy files).
	 */
	public AES(byte[] key) throws InvalidKeyException {
		try {
			decryptCipher = Cipher.getInstance("AES/ECB/NoPadding");
			decryptCipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(key, "AES"));
			encryptCipher = Cipher.getInstance("AES/ECB/NoPadding");
			encryptCipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(key, "AES"));
		} catch (NoSuchAlgorithmException | NoSuchPaddingException ignore) {
			// exceptions should not be occurred
		}
	}

	/**
	 * Create object with initialized cryptographic cipher
	 * <p>
	 * The key string will convert to UTF-8 byte array
	 *
	 * @param key
	 *          the key
	 * @throws InvalidKeyException
	 *           if the given key is inappropriate for initializing this cipher, or
	 *           requires algorithm parameters that cannot be determined from the
	 *           given key, or if the given key has a keysize that exceeds the
	 *           maximum allowable keysize (as determined from the configured
	 *           jurisdiction policy files).
	 */
	public AES(String key) throws InvalidKeyException {
		this(key.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Encrypts data
	 *
	 * @param data
	 *          data to be encrypted
	 * @return an encrypted data in byte array
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 */
	public byte[] encrypt(byte[] data) throws IllegalBlockSizeException {
		try {
			return encryptCipher.doFinal(data);
		} catch (BadPaddingException e) {
			// exception should not be occurred
			return new byte[0];
		}
	}

	/**
	 * Encrypts data
	 *
	 * @param plaintext
	 *          data to be encrypted
	 * @return an encrypted data in byte array
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 */
	public byte[] encrypt(String plaintext) throws IllegalBlockSizeException {
		return encrypt(plaintext.getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Decrypts data
	 *
	 * @param encrypted
	 *          data to be decrypted
	 * @return a decrypted byte array
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 * @throws BadPaddingException
	 *           if this cipher is in decryption mode, and (un)padding has been
	 *           requested, but the decrypted data is not bounded by the appropriate
	 *           padding bytes
	 */
	public byte[] decrypt(byte[] encrypted) throws IllegalBlockSizeException, BadPaddingException {
		return decryptCipher.doFinal(encrypted);
	}

	/**
	 * Decrypts data
	 *
	 * @param hexString
	 *          data to be decrypted in hexadecimal format
	 * @return a decrypted byte array
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 * @throws BadPaddingException
	 *           if this cipher is in decryption mode, and (un)padding has been
	 *           requested, but the decrypted data is not bounded by the appropriate
	 *           padding bytes
	 */
	public byte[] decrypt(String hexString) throws IllegalBlockSizeException, BadPaddingException {
		return decrypt(Util.hexToBytes(hexString));
	}

	/**
	 * Decrypts data
	 *
	 * @param encrypted
	 *          data to be decrypted
	 * @return a plaintext string
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 * @throws BadPaddingException
	 *           if this cipher is in decryption mode, and (un)padding has been
	 *           requested, but the decrypted data is not bounded by the appropriate
	 *           padding bytes
	 */
	public String decryptToString(byte[] encrypted) throws IllegalBlockSizeException, BadPaddingException {
		return new String(decrypt(encrypted), StandardCharsets.UTF_8);
	}

	/**
	 * Decrypts data
	 *
	 * @param hexString
	 *          data to be decrypted in hexadecimal format
	 * @return a plaintext string
	 * @throws IllegalBlockSizeException
	 *           if this cipher is a block cipher, no padding has been requested
	 *           (only in encryption mode), and the total input length of the data
	 *           processed by this cipher is not a multiple of block size; or if
	 *           this encryption algorithm is unable to process the input data
	 *           provided.
	 * @throws BadPaddingException
	 *           if this cipher is in decryption mode, and (un)padding has been
	 *           requested, but the decrypted data is not bounded by the appropriate
	 *           padding bytes
	 */
	public String decryptToString(String hexString) throws IllegalBlockSizeException, BadPaddingException {
		return decryptToString(Util.hexToBytes(hexString));
	}

}
