package ibis.maestro;

public interface PacketReceiveListener<T> {
    void packetReceived( PacketUpcallReceivePort<T> p, T packet );
}
