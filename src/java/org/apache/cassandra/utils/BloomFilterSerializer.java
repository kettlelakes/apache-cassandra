package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.io.ICompactSerializer;

class BloomFilterSerializer implements ICompactSerializer<BloomFilter>
{
    public void serialize(BloomFilter bf, DataOutputStream dos)
            throws IOException
    {
        dos.writeInt(bf.getHashCount());
        ObjectOutputStream oos = new ObjectOutputStream(dos);
        oos.writeObject(bf.bitset);
        oos.flush();
    }

    public BloomFilter deserialize(DataInputStream dis) throws IOException
    {
        int hashes = dis.readInt();
        ObjectInputStream ois = new ObjectInputStream(dis);
        try
        {
          OpenBitSet bs = (OpenBitSet) ois.readObject();
          return new BloomFilter(hashes, bs);
        }
        catch (ClassNotFoundException e)
        {
          throw new RuntimeException(e);
        }
    }
}


