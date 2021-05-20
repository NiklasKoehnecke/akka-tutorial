package de.hpi.ddm.actors;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import akka.NotUsed;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.twitter.chill.KryoPool;
import de.hpi.ddm.singletons.KryoPoolSingleton;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class LargeMessageProxy extends AbstractLoggingActor {

    ////////////////////////
    // Actor Construction //
    ////////////////////////

    public static final String DEFAULT_NAME = "largeMessageProxy";

    public static Props props() {
        return Props.create(LargeMessageProxy.class);
    }

    ////////////////////
    // Actor Messages //
    ////////////////////

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LargeMessage<T> implements Serializable {
        private static final long serialVersionUID = 2940665245810221108L;
        private T message;
        private ActorRef receiver;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class BytesMessage implements Serializable {
        private static final long serialVersionUID = 4057807743872319842L;
        private byte[] bytes;
    }

    /////////////////
    // Actor State //
    /////////////////
    private static List<BytesMessage> data;
    /////////////////////
    // Actor Lifecycle //
    /////////////////////

    ////////////////////
    // Actor Behavior //
    ////////////////////

    static class StreamInitialized {
    }

    @Data
    @AllArgsConstructor
    static class StreamCompleted {
        private final ActorRef receiver;
        private final ActorRef sender;
    }

    @Data
    @AllArgsConstructor
    static class StreamFailure {
        private final Throwable cause;
    }

    enum Ack {
        INSTANCE;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LargeMessage.class, this::handle)
                .match(
                        StreamInitialized.class,
                        init -> {
                            log().info("Stream initialized");
                            data = new ArrayList<>();
                            sender().tell(Ack.INSTANCE, self());
                        })
                .match(
                        BytesMessage.class,
                        element -> {
                            //log().info("Received element: {}", element);
                            data.add(element);
                            sender().tell(Ack.INSTANCE, self());
                        })
                .match(
                        StreamCompleted.class, this::handle)
                .match(
                        StreamFailure.class,
                        failed -> {
                            log().error(failed.getCause(), "Stream failed!");
                        })
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(LargeMessage<?> largeMessage) {
        Object message = largeMessage.getMessage();
        ActorRef receiver = largeMessage.getReceiver();
        ActorSelection receiverProxy = this.context().actorSelection(receiver.path().child(DEFAULT_NAME));
        // TODO: Maybe change with communication

        CompletionStage<ActorRef> newRef = receiverProxy.resolveOne(Duration.ofSeconds(1));
        try {
            ActorRef x = newRef.toCompletableFuture().get();

            log().info("Start sending msg to " + x);

            KryoPool serializer = KryoPoolSingleton.get();
            this.log().info("Found class when encoding " + serializer.hasRegistration(message.getClass()));
            byte[] output = serializer.toBytesWithClass(message);
            //todo maybe try out without class. Alternatively register it
            // see https://github.com/altoo-ag/akka-kryo-serialization
            Iterable<BytesMessage> groupedOutput = group(output, 100);

            Source<BytesMessage, NotUsed> source = Source.from(groupedOutput);
            Sink<BytesMessage, NotUsed> sink = Sink.actorRefWithBackpressure(x,
                    new StreamInitialized(),
                    Ack.INSTANCE,
                    new StreamCompleted(receiver, getSender()),
                    ex -> new StreamFailure(ex));
            source.runWith(sink, this.getContext().getSystem());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private void handle(StreamCompleted message) {
        this.log().info("StreamCompleted");
        KryoPool serializer = KryoPoolSingleton.get();
        ActorRef receiver = message.getReceiver();

        Object originalMessage = serializer.fromBytes(convertMessages());
        //LargeMessage originalMessage = new LargeMessage(serializer.fromBytes(convertMessages()), receiver);
        receiver.tell(originalMessage, message.getSender());
        this.log().info("Send answer");
    }

    private static byte[] convertMessages() {
        int totalBytes = (data.size() - 1) * data.get(0).getBytes().length + data.get(data.size() - 1).getBytes().length;
        byte[] bytes = new byte[totalBytes];
        int i = 0;
        //TODO concurrentModificationException
        for (BytesMessage m : data) {
            System.arraycopy(m.getBytes(), 0, bytes, i, m.getBytes().length);
            i += m.getBytes().length;
        }
        return bytes;
    }

    private static List<BytesMessage> group(byte[] byteArray, int size) {
        List<BytesMessage> groupedBytes = new ArrayList<>();
        for (int i = 0; i < byteArray.length; i += size) {
            int sizeOfArray = Math.min(byteArray.length - i, size);
            byte[] sliced = Arrays.copyOfRange(byteArray, i, i + sizeOfArray);
            groupedBytes.add(new BytesMessage(sliced));
        }
        return groupedBytes;
    }

}
