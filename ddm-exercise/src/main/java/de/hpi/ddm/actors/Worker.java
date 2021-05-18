package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import de.hpi.ddm.systems.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import akka.cluster.Member;
import akka.cluster.MemberStatus;

public class Worker extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "worker";

    public static Props props() {
        return Props.create(Worker.class);
    }

    public Worker() {
        this.cluster = Cluster.get(this.context().system());
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class HintDecryptMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private String availableCharacters; // Length n - x
        private String prefix; // Length x
        private List<String> hints;
    }

    /////////////////
    // Actor State //
    /////////////////

    private Member masterSystem;
    private final Cluster cluster;
    private final ActorRef largeMessageProxy;
    private long registrationTime;
    private static final String NO_DECRYPTION = "";

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);

        this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
    }

    @Override
    public void postStop() {
        this.cluster.unsubscribe(this.self());
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, this::handle)
                .match(MemberUp.class, this::handle)
                .match(MemberRemoved.class, this::handle)
                .match(HintDecryptMessage.class, this::handle)
                // TODO: Add further messages here to share work between Master and Worker actors
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CurrentClusterState message) {
        message.getMembers().forEach(member -> {
            if (member.status().equals(MemberStatus.up()))
                this.register(member);
        });
    }

    private void handle(MemberUp message) {
        this.register(message.member());
    }

    private void register(Member member) {
        if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
            this.masterSystem = member;

            this.getContext()
                    .actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
                    .tell(new Master.RegistrationMessage(), this.self());

            this.registrationTime = System.currentTimeMillis();
        }
    }

    private void handle(MemberRemoved message) {
        if (this.masterSystem.equals(message.member()))
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    private void handle(HintDecryptMessage message) {
        this.log().info("Received work! No longer living on the streets :).");
        HashMap<Integer, Character> result = decryptHint(message.getHints(), message.getAvailableCharacters(), message.prefix);
        getSender().tell(new Master.ResultMessage(result), this.getSelf());
        this.log().info("Finished with work.");
    }

    private HashMap<Integer, Character> decryptHint(List<String> hints, String availableCharacters, String prefix) {
        List<String> correctCombinations = getValidPermutation(availableCharacters.toCharArray(), availableCharacters.length(), prefix, hints);
        HashMap<Integer, Character> validCombinations = new HashMap<>();
        for (int i = 0; i < hints.size(); i++) {
            String decryptedHint = correctCombinations.get(i);

            if (decryptedHint.equals(NO_DECRYPTION))
                continue;
            for (char c : availableCharacters.toCharArray()) {
                if (decryptedHint.indexOf(c) == -1) {
                    validCombinations.put(i, c);
                }
            }
        }
        return validCombinations;
    }

    private static String hash(String characters) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(characters).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuffer = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private static void swap(char[] input, int a, int b) {
        char tmp = input[a];
        input[a] = input[b];
        input[b] = tmp;
    }

    public static List<String> getValidPermutation(char[] elements, int n, String prefix, List<String> encryptedHints) {
        List<String> decryptedHints = new ArrayList<>(Collections.nCopies(encryptedHints.size(), NO_DECRYPTION));
        int[] indexes = new int[n];
        for (int i = 0; i < n; i++) {
            indexes[i] = 0;
        }
        checkHashes(elements, prefix, encryptedHints, decryptedHints);

        int i = 0;
        while (i < n) {
            if (indexes[i] < i) {
                swap(elements, i % 2 == 0 ? 0 : indexes[i], i);

                checkHashes(elements, prefix, encryptedHints, decryptedHints);

                indexes[i]++;
                i = 0;
            } else {
                indexes[i] = 0;
                i++;
            }
        }

        return decryptedHints;
    }

    private static void checkHashes(char[] elements, String prefix, List<String> encryptedHints, List<String> decryptedHints) {
        String currentCombination = prefix + new String(elements).substring(1);
        String currentHash = Worker.hash(currentCombination);
        for (int hintIndex = 0; hintIndex < encryptedHints.size(); hintIndex++) {
            if (currentHash.equals(encryptedHints.get(hintIndex))) {
                decryptedHints.set(hintIndex, currentCombination);
            }
        }
    }

    public static List<String> getAllCombinations(char[] set, int k) {
        List<String> allPossibilites = new ArrayList<>();
        int n = set.length;
        printAllKLengthRec(set, "", n, k, allPossibilites);
        return allPossibilites;
    }

    // The main recursive method
    // to print all possible
    // strings of length k
    private static void printAllKLengthRec(char[] set,
                                           String prefix,
                                           int n, int k, List<String> allPossibilities) {
        // Base case: k is 0,
        // print prefix
        if (k == 0) {
            allPossibilities.add(prefix);
            return;
        }

        // One by one add all characters
        // from set and recursively
        // call for k equals to k-1
        for (int i = 0; i < n; ++i) {

            // Next character of input added
            String newPrefix = prefix + set[i];

            // k is decreased, because
            // we have added a new character
            printAllKLengthRec(set, newPrefix,
                    n, k - 1, allPossibilities);
        }
    }
}