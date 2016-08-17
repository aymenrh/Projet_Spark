package fr.mediametrie.internet.streaming.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.io.IOContext;
import javax.annotation.Nonnull;

/**
 * Jackson {@link JsonFactory} implementation to lower case all the keys when deserializing json.
 */
public class CustomParserJsonFactory extends JsonFactory {

    private Function<JsonParser, JsonParser> parserFactory;

    public CustomParserJsonFactory(@Nonnull Function<JsonParser, JsonParser> parserFactory) {
        this.parserFactory = parserFactory;
    }

    @Override
    protected JsonParser _createParser(byte[] data, int offset, int len, IOContext ctxt) throws IOException {
        return parserFactory.apply(super._createParser(data, offset, len, ctxt));
    }

    @Override
    protected JsonParser _createParser(InputStream in, IOContext ctxt) throws IOException {
        return parserFactory.apply(super._createParser(in, ctxt));
    }

    @Override
    protected JsonParser _createParser(Reader r, IOContext ctxt) throws IOException {
        return parserFactory.apply(super._createParser(r, ctxt));
    }

    @Override
    protected JsonParser _createParser(char[] data, int offset, int len, IOContext ctxt, boolean recyclable)
            throws IOException {
        return parserFactory.apply(super._createParser(data, offset, len, ctxt, recyclable));
    }

}
