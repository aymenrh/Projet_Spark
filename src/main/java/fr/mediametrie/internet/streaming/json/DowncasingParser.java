package fr.mediametrie.internet.streaming.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.util.JsonParserDelegate;

/**
 * {@link JsonParser} implementation to lower case the key when parsin json.
 */
public class DowncasingParser extends JsonParserDelegate {

    public DowncasingParser(JsonParser d) {
        super(d);
    }

    @Override
    public String getCurrentName() throws IOException {
        if (getCurrentTokenId() == JsonTokenId.ID_FIELD_NAME) {
            return delegate.getCurrentName().toLowerCase();
        }
        return delegate.getCurrentName();
    }

    @Override
    public String getText() throws IOException {
        if (getCurrentTokenId() == JsonTokenId.ID_FIELD_NAME) {
            return delegate.getText().toLowerCase();
        }
        return delegate.getText();
    }
}