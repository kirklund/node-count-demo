package io.github.kirklund.nodecountdemo;

import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.DUnitLauncher;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.geode.test.dunit.VM.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;

/**
 * There are nodes/ set of computers(A, B, C, D, E, F for example) in the range of 1 to a million connected in a bus-type network.
 *
 * Like such:
 *
 * A -- B -- C -- D -- E -- F
 *
 * Each node can communicate with its direct neighbor, using the following provided methods:
 *
 *   boolean hasLeft();
 *   boolean hasRight();
 *   void sendToLeft(int val);
 *   void sendToRight(int val);
 *
 * When the program is run on a random node, it should print the number of nodes in the network.
 *
 * You can make use of the above methods in your own program which will have a main() method and 2 callbacks that you need to implement:
 *
 *   void receiveFromLeft(int val);
 *   void receiveFromRight(int val);
 */
public class NodeCountDemoDistributedTest implements Serializable {

    @BeforeClass
    public static void beforeClass() {
        DUnitLauncher.launchIfNeeded(false);
        getVM(4);
        getVM(5);
        assertThat(getVMCount()).isEqualTo(6);
    }

    @Rule
    public DistributedRule distributedRule = new DistributedRule(6);
    @Rule
    public DistributedReference<Node> node = new DistributedReference<>(6);

    private final VM vmA = getVM(0);
    private final VM vmB = getVM(1);
    private final VM vmC = getVM(2);
    private final VM vmD = getVM(3);
    private final VM vmE = getVM(4);
    private final VM vmF = getVM(5);

    @Test
    public void countNodesFromC() {
        vmA.invoke(() -> node.set(Node.create("nodeA", null, vmB)));
        vmB.invoke(() -> node.set(Node.create("nodeB", vmA, vmC)));
        vmC.invoke(() -> node.set(Node.create("nodeC", vmB, vmD)));
        vmD.invoke(() -> node.set(Node.create("nodeD", vmC, vmE)));
        vmE.invoke(() -> node.set(Node.create("nodeE", vmD, vmF)));
        vmF.invoke(() -> node.set(Node.create("nodeF", vmE, null)));

        vmC.invoke(() -> {
            Node localNode = node.get();
            localNode.sender = true;
            localNode.sendToRight(1);
            assertThat(localNode.nodeCount).isEqualTo(6);
        });

    }

    public static class Node implements Receiver, Serializable {

        private static final AtomicReference<Node> STATIC = new AtomicReference<>();
        private final String name;
        private final VM left;
        private final VM right;
        private boolean sender;
        private int nodeCount = 0;

        public static Node create(String name, VM left, VM right) {
            Node node = new Node(name, left, right);
            STATIC.set(node);
            return node;
        }

        public Node(String name, VM left, VM right) {
            this.name = name;
            this.left = left;
            this.right = right;
        }

        private void debug(String operation, int val) {
            System.out.println(String.format("%s: %s val=%d, nodeCount=%d, sender=%b", name, operation, val, nodeCount, sender));
        }

        @Override
        public void receiveFromLeft(int val) {
            debug("receiveFromLeft", val);
            if (sender) {
                nodeCount += val;
                return;
            } else {
                val++;
            }

            // only increment value when passing to the right
            if (hasRight()) {
                // continue sending to the right
                sendToRight(val);
            } else {
                //start sending to the left
                sendToLeft(val);
            }
        }

        @Override
        public void receiveFromRight(int val) {
            debug("receiveFromRight", val);
            // do NOT change value when returning value to the left
            if (hasLeft()) {
                // continue returning to the left
                sendToLeft(val);
            } else {
                // start sending to the right
                val++;
                sendToRight(val);
            }
        }

        public boolean hasLeft() {
            return left != null;
        }

        public boolean hasRight() {
            return right != null;
        }

        public void sendToLeft(int val) {
            assertThat(hasLeft()).isTrue();
            left.invoke(() -> STATIC.get().receiveFromRight(val));
        }

        public void sendToRight(int val) {
            assertThat(hasRight()).isTrue();
            right.invoke(() -> STATIC.get().receiveFromLeft(val));
        }
    }

    public interface Receiver {
        void receiveFromLeft(int val);
        void receiveFromRight(int val);
    }

}
