package algorithms;

import models.Node;
import models.Request;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

public class LoadBalanceConsistentHashing implements Router {
    private final Map<Node, List<Long>> nodePositions; // to keep track of all the positions of the node(ServerA can have 10 locations), will be useful during deletion 
    private final ConcurrentSkipListMap<Long, Node> nodeMappings; // to navigate to next available node to handle requests
    private final Function<String, Long> hashFunction;
    private final int pointMultiplier;//from how many positions a node is serving the request


    public ConsistentHashing(final Function<String, Long> hashFunction,
                             final int pointMultiplier) {
        if (pointMultiplier == 0) {
            throw new IllegalArgumentException();
        }
        this.pointMultiplier = pointMultiplier;
        this.hashFunction = hashFunction;
        this.nodePositions = new ConcurrentHashMap<>();
        this.nodeMappings = new ConcurrentSkipListMap<>();
    }

    // To add a node, we need
    public void addNode(Node node) {
        nodePositions.put(node, new CopyOnWriteArrayList<>());//new node will go to new set of positions
        for (int i = 0; i < pointMultiplier; i++) {
            for (int j = 0; j < node.getWeight(); j++) {
                final var point = hashFunction.apply((i * pointMultiplier + j) + node.getId());//calculate position of new node
                nodePositions.get(node).add(point);//adding position point to keep track
                nodeMappings.put(point, node);//map that point to server node A, so when a request will come to a point, it will come to server node A
            }
        }
    }

    public void removeNode(Node node) {
        for (final Long point : nodePositions.remove(node)) {
            nodeMappings.remove(point);// remove all the points/positions that the node was serving
        }
    }

    public Node getAssignedNode(Request request) {
        final var key = hashFunction.apply(request.getId());// get the point of the new request, it will go to the next nearest point where server is present
        final var entry = nodeMappings.higherEntry(key);//get the point(server) available to serve the request
        if (entry == null) {
            return nodeMappings.firstEntry().getValue();//no point available, go to first entry
        } else {
            return entry.getValue();
        }
    }
}
