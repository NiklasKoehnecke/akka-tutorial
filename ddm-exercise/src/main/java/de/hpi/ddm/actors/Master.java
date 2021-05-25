package de.hpi.ddm.actors;

import java.io.Serializable;
import java.util.*;

import akka.actor.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


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
        this.hintsInLine = new HashMap<>();
        this.charactersNotInPasswords = new HashMap<>();
        this.encryptedPasswords = new HashMap<>();
        this.numDecryptedPasswords = new HashMap<>();
        this.waitingForReader = true;
        this.finishedReading = true;
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
        private int batchID;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PasswordResultMessage implements Serializable {
        private static final long serialVersionUID = 8343040942748609592L;
        private String password;
        private int batchID;
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
    private final Queue<Worker.WorkerTask> workload;
    private final LinkedList<ActorRef> idleWorker;
    private final Map<ActorRef, Worker.WorkerTask> busyWorkers;
    private final HashMap<Integer, List<Integer>> hintsInLine;
    private final HashMap<Integer, List<Character>> charactersNotInPasswords;
    private final HashMap<Integer, List<String>> encryptedPasswords;
    private final HashMap<Integer, Integer> numDecryptedPasswords;
    private String availableCharacters;
    private int passwordSize;
    private int batchIndex;
    private boolean waitingForReader;
    private boolean finishedReading;

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
                .match(PasswordResultMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    protected void handle(StartMessage message) {
        this.startTime = System.currentTimeMillis();

        this.reader.tell(new Reader.ReadMessage(), this.self());
    }

    protected void handle(BatchMessage message) {

        if (message.getLines().isEmpty()) {
            finishedReading = true;
            checkIfFinished();
            return;
        }

        this.batchIndex++;

        availableCharacters = message.getLines().get(0)[2];
        passwordSize = Integer.parseInt(message.getLines().get(0)[3]);
        ArrayList<Integer> hintsInLine = new ArrayList<>();
        ArrayList<String> encryptedPasswords = new ArrayList<>();
        ArrayList<Character> charactersNotInPasswords;

        List<String> hints = new ArrayList<>();
        for (String[] line : message.getLines()) {
            hints.addAll(Arrays.asList(line).subList(5, line.length));
            hintsInLine.add(line.length - 5);
            encryptedPasswords.add(line[4]);
        }

        int totalSize = hintsInLine.stream().reduce(0, Integer::sum);
        charactersNotInPasswords = new ArrayList<>(Collections.nCopies(totalSize, NO_CHAR));
        this.hintsInLine.put(this.batchIndex, hintsInLine);
        this.encryptedPasswords.put(this.batchIndex, encryptedPasswords);
        this.charactersNotInPasswords.put(this.batchIndex, charactersNotInPasswords);
        this.numDecryptedPasswords.put(this.batchIndex, 0);

        for (char char1 : availableCharacters.toCharArray()) {
            for (char char2 : availableCharacters.toCharArray()) {
                if (char1 == char2) continue;
                String newAvailable = availableCharacters
                        .replaceFirst(Character.toString(char1), "")
                        .replaceFirst(Character.toString(char2), "");
                String leftOut = new String(new char[]{char1, char2});
                Worker.HintDecryptMessage msg = new Worker.HintDecryptMessage(newAvailable, leftOut, hints, this.batchIndex);
                this.addWork(msg);
            }
        }
        this.waitingForReader = false;

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
        gotResultsFrom(getSender());
        List<Character> charactersNotInPasswords = this.charactersNotInPasswords.get(message.getBatchID());
        List<Integer> hintsInLine = this.hintsInLine.get(message.getBatchID());

        for (Integer position : message.missingCharacters.keySet()) {
            charactersNotInPasswords.set(position, message.missingCharacters.get(position));
        }

        int missingCharactersFound = (int) charactersNotInPasswords.stream()
                .filter(character -> !character.equals(NO_CHAR)).count();
        int totalSize = charactersNotInPasswords.size();//hintsInLine.stream().reduce(0, Integer::sum);
        this.log().info("Waiting for " + (totalSize - missingCharactersFound) + " Results in batch " + message.getBatchID());
        if (missingCharactersFound != totalSize) {
            return;
        }

        this.log().info("Calculate Passwords");
        List<String> encryptedPasswords = this.encryptedPasswords.get(message.getBatchID());

        List<List<Character>> deflattened = deflattenDecryptedHints(charactersNotInPasswords, hintsInLine);
        for (int i = 0; i < encryptedPasswords.size(); i++) {
            List<Character> forbiddenChars = deflattened.get(i);
            String password = encryptedPasswords.get(i);
            String withForbiddenRemoved = availableCharacters;
            for (Character forbiddenChar : forbiddenChars) {
                withForbiddenRemoved = withForbiddenRemoved.replace(Character.toString(forbiddenChar), "");
            }
            //TODO split up passwords maybe
            this.addWork(new Worker.PasswordDecryptMessage(password, withForbiddenRemoved, passwordSize, message.getBatchID()));
        }
    }

    protected void handle(PasswordResultMessage message) {
        gotResultsFrom(getSender());
        final int batchID = message.getBatchID();
        this.collector.tell(new Collector.CollectMessage(message.getPassword()), this.self());
        this.numDecryptedPasswords.computeIfPresent(batchID, (integer, integer2) -> integer2 + 1);
        if (this.numDecryptedPasswords.get(batchID) == this.encryptedPasswords.get(batchID).size()) {
            this.encryptedPasswords.remove(batchID);
            this.numDecryptedPasswords.remove(batchID);
            this.charactersNotInPasswords.remove(batchID);
            this.hintsInLine.remove(batchID);
        }
        checkIfFinished();
    }

    protected void checkIfFinished() {
        if (this.finishedReading && this.numDecryptedPasswords.size() == 0) {
            this.terminate();
        }
    }

    private void giveWorkerAJob(ActorRef worker) {
        if (workload.size() == 0) {
            idleWorker.add(worker);
            if (!this.waitingForReader) {
                this.reader.tell(new Reader.ReadMessage(), this.self());
                this.waitingForReader = true;
            }
        } else
            this.giveWorkerAJob(worker, workload.remove());
    }

    private void giveWorkerAJob(ActorRef worker, Worker.WorkerTask work) {
        //this.largeMessageProxy.tell(new LargeMessageProxy.LargeMessage<>(work, worker), this.self());
        worker.tell(work, this.self());
        this.busyWorkers.put(worker, work);
    }

    private void addWork(Worker.WorkerTask work) {
        if (this.idleWorker.isEmpty())
            this.workload.add(work);
        else
            giveWorkerAJob(idleWorker.remove(), work);
    }

    private void gotResultsFrom(ActorRef worker) {
        this.log().info("Received result from " + worker);
        busyWorkers.remove(worker);
        this.giveWorkerAJob(worker);
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
}
