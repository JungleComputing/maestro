package compression;

public interface BlockOutput {
    public byte [] storeBlock(byte [] buffer, int len, boolean needNew, boolean endOfBlock);
}
