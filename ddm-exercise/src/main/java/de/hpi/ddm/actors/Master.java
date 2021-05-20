package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.stream.Collectors;


public class Master extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "master";
    private static final Character NO_CHAR = '#';

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
        this.busyWorkers = new HashMap<>();
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
        private HashMap<Integer, Character> missingCharacters;
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
    private final LinkedList<ActorRef> idleWorker;
    private final Map<ActorRef, Worker.HintDecryptMessage> busyWorkers;
    private List<Integer> hintSizes;
    private List<Character> missingCharacters;
    private String availableCharacters;
    private List<String> passwords;
    private int passwordSize;

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
                .match(ResultMessage.class, this::handle)
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


        availableCharacters = message.getLines().get(0)[2];
        passwordSize = Integer.parseInt(message.getLines().get(0)[3]);
        this.hintSizes = new ArrayList<>();
        this.passwords = new ArrayList<>();
        this.missingCharacters = new ArrayList<>();

        List<String> hints = new ArrayList<>();
        for (String[] line : message.getLines()) {
            for (int i = 5; i < line.length; i++) {
                hints.add(line[i]);
            }
            hintSizes.add(line.length - 5);
            passwords.add(line[4]);
        }

        int totalSize = hintSizes.stream().reduce(0, Integer::sum);
        missingCharacters = new ArrayList<>(Collections.nCopies(totalSize, NO_CHAR));

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
        //TODO why do workers die :(
        for (int i = 0; i < idleWorker.size(); i++) {
            giveWorkerAJob(idleWorker.remove());
        }
    }

    private void giveWorkerAJob(ActorRef worker) {
        if (workload.size() == 0) {
            idleWorker.add(worker);
        } else {
            Worker.HintDecryptMessage work = workload.remove();
            //this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(work, worker), this.self());
            worker.tell(work, this.self());
            this.busyWorkers.put(worker, work);
        }
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
        ActorRef unsubscriber = message.getActor();
        this.context().unwatch(unsubscriber);
        this.workers.remove(unsubscriber);
        if (this.busyWorkers.containsKey(unsubscriber)) {
            this.workload.add(this.busyWorkers.remove(unsubscriber));
        }
        int idleIndex = this.idleWorker.indexOf(unsubscriber);
        if (idleIndex != -1)
            this.idleWorker.remove(idleIndex);
        //TODO distribute work if everyone is already idle

        this.log().info("Unregistered {}", message.getActor());
    }

    protected void handle(ResultMessage message) {
        busyWorkers.remove(getSender());
        this.log().info("Received result from " + getSender());
        giveWorkerAJob(getSender());

        for (Integer position : message.missingCharacters.keySet()) {
            missingCharacters.set(position, message.missingCharacters.get(position));
        }

        int missingCharactersFound = missingCharacters.stream()
                .filter(character -> !character.equals(NO_CHAR))
                .collect(Collectors.toList())
                .size();
        int totalSize = hintSizes.stream().reduce(0, Integer::sum);
        System.out.println("Waiting for " + (totalSize - missingCharactersFound) + " Results");
        if (missingCharactersFound != totalSize) {
            return;
        }
        System.out.println("Calculate Passwords");

        List<List<Character>> deflattened = deflattenDecryptedHints(missingCharacters, hintSizes);
        for (int i = 0; i < passwords.size(); i++) {
            List<Character> forbiddenChars = deflattened.get(i);
            String password = passwords.get(i);
            String withForbiddenRemoved = availableCharacters;
            for (Character forbiddenChar : forbiddenChars) {
                withForbiddenRemoved = withForbiddenRemoved.replace(Character.toString(forbiddenChar), "");
            }
            String pass = decryptPassword(password, withForbiddenRemoved, passwordSize);
            this.collector.tell(new Collector.CollectMessage(pass), this.self());
        }
        System.out.println("Finished");
        this.reader.tell(new Reader.ReadMessage(), this.self());

    }

    private static List<List<Character>> deflattenDecryptedHints(List<Character> hints, List<Integer> hintsOfLine) {
        List<List<Character>> deflattenedHints = new ArrayList<>();
        for (int i = 0; i < hintsOfLine.size(); i++) {
            deflattenedHints.add(new ArrayList<>());
        }

        int currentLine = 0;
        int hintsBeforeLine = 0;
        for (int i = 0; i < hints.size(); i++) {
            if (i >= hintsBeforeLine + hintsOfLine.get(currentLine)) {
                currentLine += 1;
                hintsBeforeLine += hintsOfLine.get(currentLine);
            }
            deflattenedHints.get(currentLine).add(hints.get(i));
        }
        return deflattenedHints;
    }

    private static String decryptPassword(String encryptedPassword, String availableCharacters, int passwordLength) {
        List<String> combinations = Worker.getAllCombinations(availableCharacters.toCharArray(), passwordLength);

        for (String combination : combinations) {
            if (Worker.hash(combination).equals(encryptedPassword)) {
                return combination;
            }
        }

        System.out.println("Found no valid password");
        return "";
    }

}
