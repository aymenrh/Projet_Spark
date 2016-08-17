package fr.mediametrie.internet.streaming.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * Utils for ZLIB operations
 */
public class ZlibUtils {

    /**
     * Compress a byte array with {@link Deflater#DEFAULT_COMPRESSION} and {@link Deflater#DEFAULT_STRATEGY} using
     * Defalte
     * 
     * @param data The data to compress
     * @return The compressed data
     * @throws IOException
     */
    public static byte[] compress(byte[] data) throws IOException {
        return compress(data, Deflater.DEFAULT_COMPRESSION, Deflater.DEFAULT_STRATEGY);
    }

    /**
     * Compress a byte array with specified compression level and strategy using Defalte
     * 
     * @param data The data to compress
     * @param compressionLevel The compression level
     * @param strategy The compression strategy
     * @return The compressed data
     * @throws IOException
     */
    public static byte[] compress(byte[] data, int compressionLevel, int strategy) throws IOException {
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.setLevel(compressionLevel);
        deflater.setStrategy(strategy);
        deflater.finish();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!deflater.finished()) {
            int count = deflater.deflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();

        return outputStream.toByteArray();
    }

    /**
     * Decompress a byte array using Inflate
     * 
     * @param data The data to decompress
     * @return The decompressed data
     * 
     * @throws IOException
     * @throws DataFormatException if the compressed data format is invalid
     */
    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
        byte[] buffer = new byte[1024];
        while (!inflater.finished()) {
            int count = inflater.inflate(buffer);
            outputStream.write(buffer, 0, count);
        }
        outputStream.close();
        return outputStream.toByteArray();
    }


}
