package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.ddm.structures.BloomFilter;
import it.unimi.dsi.fastutil.Hash;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";

    public static Props props(final ActorRef reader, final ActorRef collector) {
        return Props.create(Master.class, () -> new Master(reader, collector));
    }

    public Master(final ActorRef reader, final ActorRef collector) {
        this.reader = reader;
        this.collector = collector;
        this.workers = new ArrayList<>();
        this.largeMessageProxy = this.context().actorOf(LargeMessageProxy.props(), LargeMessageProxy.DEFAULT_NAME);
        this.workload = new LinkedList<>();
        this.idleWorker = new LinkedList<>();
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    public static class StartMessage implements Serializable {
        private static final long serialVersionUID = -50374816448627600L;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BatchMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private List<String[]> lines;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ResultMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609598L;
        private HashMap<Integer, String> decryptedHints;
    }

    @Data
    public static class RegistrationMessage implements Serializable {
        private static final long serialVersionUID = 3303081601659723997L;
    }

    /////////////////
    // Actor State //
    /////////////////

    private final ActorRef reader;
    private final ActorRef collector;
    private final List<ActorRef> workers;
    private final ActorRef largeMessageProxy;
    private final Queue<Worker.HintDecryptMessage> workload;
    private final Queue<ActorRef> idleWorker;

    private long startTime;

    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    @Override
    public void preStart() {
        Reaper.watchWithDefaultReaper(this);
    }

    ////////////////////
    // Actor Behavior //
    ////////////////////

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartMessage.class, this::handle)
                .match(BatchMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .match(RegistrationMessage.class, this::handle)
                // TODO: Add further messages here to share work between Master and Worker actors
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {

        if (message.getLines().isEmpty()) {
            // TODO: Stop fetching lines from the Reader once an empty BatchMessage was received; we have seen all data then
            // TODO: await all results and finish
            this.terminate();
            return;
        }

        // TODO: Process the lines with the help of the worker actors
        String availableCharacters = message.getLines().get(0)[2];
        List<String> hints = new ArrayList<>();
        for (String[] line : message.getLines()) {
            for (int i = 5; i < line.length; i++) {
                hints.add(line[i]);
            }
        }

        for (char char1 : availableCharacters.toCharArray()) {
            for (char char2 : availableCharacters.toCharArray()) {
                if (char1 == char2) continue;
                String newAvailable = availableCharacters
                        .replaceFirst(Character.toString(char1), "")
                        .replaceFirst(Character.toString(char2), "");
                String leftOut = new String(new char[]{char1, char2});
                Worker.HintDecryptMessage msg = new Worker.HintDecryptMessage(newAvailable, leftOut, hints);
                workload.add(msg);
            }
        }
        for(int i=0; i<idleWorker.size(); i++){
            giveWorkerAJob(idleWorker.remove());
        }

        // TODO: Send (partial) results to the Collector
        //this.collector.tell(new Collector.CollectMessage("If I had results, this would be one."), this.self());

        // TODO: Fetch further lines from the Reader
        this.reader.tell(new Reader.ReadMessage(), this.self());

    }

    private void giveWorkerAJob(ActorRef worker) {
        if (workload.size() == 0) {
            idleWorker.add(worker);
        } else {
            this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(workload.remove(), this.sender()), this.self());
        }
    }

    private String getPassword(String[] line) {
        // Parse Line
        String encryptedPassword = line[4];
        String availableCharacters = line[2];

        int passwordLength = Integer.parseInt(line[3]);
        List<String> hints = new ArrayList<>();
        for (int i = 5; i < line.length; i++) {
            hints.add(line[i]);
        }

        this.log().info("Start decrypting hints");
        String charsToGenPassword = decryptHint(hints, availableCharacters);
        this.log().info("decrypted hints, combinations: " + charsToGenPassword);

        String password = decryptPassword(encryptedPassword, charsToGenPassword, passwordLength);
        return password;
    }


    private String decryptHint(List<String> hints, String availableCharacters) {
        List<String> correctCombinations = Worker.getValidPermutation(availableCharacters.toCharArray(), availableCharacters.length(), hints);
        String validCharacters = availableCharacters;
        for (String decryptedHint : correctCombinations) {
            ;
            for (char c : availableCharacters.toCharArray()) {
                if (decryptedHint.indexOf(c) == -1) {
                    validCharacters = validCharacters.replaceFirst(Character.toString(c), "");
                }
            }
        }

        return validCharacters;
    }

    private String decryptPassword(String encryptedPassword, String availableCharacters, int passwordLength) {
        List<String> combinations = Worker.getAllCombinations(availableCharacters.toCharArray(), passwordLength);

        for (String combination : combinations) {
            if (Worker.hash(combination).equals(encryptedPassword)) {
                return combination;
            }
        }

        this.log().error("Found no valid password for {}", encryptedPassword);
        return "";
    }


    protected void terminate() {
        this.collector.tell(new Collector.PrintMessage(), this.self());

        this.reader.tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.collector.tell(PoisonPill.getInstance(), ActorRef.noSender());

        for (ActorRef worker : this.workers) {
            this.context().unwatch(worker);
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }

        this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());

        long executionTime = System.currentTimeMillis() - this.startTime;
        this.log().info("Algorithm finished in {} ms", executionTime);
    }

    protected void handle(RegistrationMessage message) {
        this.context().watch(this.sender());
        this.workers.add(this.sender());
        this.log().info("Registered {}", this.sender());
        giveWorkerAJob(this.sender());
    }

    protected void handle(Terminated message) {
        this.context().unwatch(message.getActor());
        this.workers.remove(message.getActor());
        this.log().info("Unregistered {}", message.getActor());
    }

}
