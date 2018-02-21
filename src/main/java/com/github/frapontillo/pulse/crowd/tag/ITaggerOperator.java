package com.github.frapontillo.pulse.crowd.tag;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.crowd.data.entity.Tag;
import com.github.frapontillo.pulse.rx.PulseSubscriber;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.util.PulseLogger;
import org.apache.logging.log4j.Logger;
import rx.Observable;
import rx.Subscriber;

import java.util.List;

/**
 * @author Francesco Pontillo
 */
public abstract class ITaggerOperator implements Observable.Operator<Message, Message> {
    private IPlugin plugin;
    private GenericTaggerConfig config;
    private final static Logger logger = PulseLogger.getLogger(ITaggerOperator.class);


    public ITaggerOperator(IPlugin plugin) {
        this.plugin = plugin;
    }

    public void setConfig(GenericTaggerConfig config) {
        this.config = config;
    }

    @Override public Subscriber<? super Message> call(Subscriber<? super Message> subscriber) {
        return new PulseSubscriber<Message>(subscriber) {
            @Override public void onNext(Message message) {
                plugin.reportElementAsStarted(message.getId());
                message = tagMessage(message);
                plugin.reportElementAsEnded(message.getId());
                subscriber.onNext(message);
            }

            @Override public void onCompleted() {
                plugin.reportPluginAsCompleted();
                super.onCompleted();
            }

            @Override public void onError(Throwable e) {
                plugin.reportPluginAsErrored();
                super.onError(e);
            }
        };
    }

    /**
     * Starts an asynchronous tagging process loading an {@link List} of {@link Tag}s.
     *
     * @param text     {@link String} text to tag
     * @param language {@link String} language of the text to tag (can be discarded by some
     *                 implementations)
     *
     * @return {@link List <net.frakbot.crowdpulse.data.entity.Tag>}
     */
    public List<Tag> getTags(String text, String language) {
        List<Tag> tags = getTagsImpl(text, language);
        for (Tag tag : tags) {
            tag.setLanguage(language);
        }
        return tags;
    }

    /**
     * Tag a {@link Message} by calling {@link #getTags(String, String)} and setting all the {@link
     * Tag}s to the original message.
     *
     * @param message The {@link Message} to tag.
     *
     * @return The tagged input {@link Message}.
     */
    public Message tagMessage(Message message) {
        List<Tag> tags;
        if (config != null) {
            switch (config.getCalculate()) {
                case GenericTaggerConfig.ALL:
                    tags = getTags(message.getText(), message.getLanguage());
                    message.addTags(tags);
                    break;
                case GenericTaggerConfig.NEW:
                    if (message.getTags() == null || message.getTags().size() == 0) {
                        tags = getTags(message.getText(), message.getLanguage());
                        message.addTags(tags);
                    } else {
                        logger.info("Message skipped (tags already exist)");
                    }
                    break;
                default:
                    tags = getTags(message.getText(), message.getLanguage());
                    message.addTags(tags);
                    break;
            }
        }
        tags = getTags(message.getText(), message.getLanguage());
        message.addTags(tags);
        return message;
    }

    /**
     * Actual {@link Tag} retrieval implementation.
     *
     * @param text     The text to add {@link Tag}s to.
     * @param language The language of the text.
     *
     * @return A {@link List<Tag>}.
     */
    protected abstract List<Tag> getTagsImpl(String text, String language);

    /**
     * Generic configuration for tagger.
     */
    interface GenericTaggerConfig {

        /**
         * Tag of all messages coming from the stream.
         */
        public static final String ALL = "all";

        /**
         * Tag the messages with no tags (property is null or array is empty).
         */
        public static final String NEW = "new";

        public String getCalculate();
        public String setCalculate();
    }
}
