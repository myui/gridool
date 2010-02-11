package gridool.communication.transport;

import gridool.GridConfiguration;
import gridool.GridNode;
import gridool.communication.CommunicationMessageListener;
import gridool.communication.GridCommunicationMessage;
import gridool.communication.GridCommunicationService;
import gridool.communication.GridTopic;
import gridool.communication.GridTransportListener;
import gridool.communication.payload.GridNodeInfo;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public abstract class CommunicationServiceBase
        implements GridCommunicationService, GridTransportListener {
    private static final Log LOG = LogFactory.getLog(CommunicationServiceBase.class);
    public static final int DEFAULT_PORT = 47100;

    protected final GridConfiguration config;
    protected final Map<GridTopic, List<CommunicationMessageListener>> listenerMap;
    protected final GridNodeInfo localNode;

    public CommunicationServiceBase(@Nonnull GridConfiguration config) {
        this.config = config;
        this.listenerMap = new IdentityHashMap<GridTopic, List<CommunicationMessageListener>>(2);
        this.localNode = config.getLocalNode();
    }

    public String getServiceName() {
        return GridCommunicationService.class.getName();
    }

    public boolean isDaemon() {
        return true;
    }

    public GridNode getLocalNode() {
        return localNode;
    }

    public void addListener(GridTopic topic, CommunicationMessageListener listener) {
        final List<CommunicationMessageListener> listeners = listenerMap.get(topic);
        if(listeners == null) {
            List<CommunicationMessageListener> listenerList = new ArrayList<CommunicationMessageListener>(2);
            listenerList.add(listener);
            listenerMap.put(topic, listenerList);
        } else {
            if(!listeners.contains(listener)) {
                listeners.add(listener);
            }
        }
    }

    public void notifyListener(GridCommunicationMessage msg) {
        final GridTopic topic = msg.getTopic();
        assert (topic != null) : msg;
        final List<CommunicationMessageListener> listeners = listenerMap.get(topic);
        boolean foundListener = false;
        for(CommunicationMessageListener listener : listeners) {
            listener.onMessage(msg, localNode);
            foundListener = true;
        }
        if(!foundListener) {
            LOG.warn("No corresponding listener of topic [" + topic + "] found for "
                    + msg.getMessageId());
        }
    }

}