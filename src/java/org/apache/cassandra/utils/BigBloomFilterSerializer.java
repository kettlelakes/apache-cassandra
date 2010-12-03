package org.apache.cassandra.utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import org.apache.cassandra.utils.obs.OpenBitSet;
import org.apache.cassandra.io.ICompactSerializer;

class BigBloomFilterSerializer implements ICompactSerializer<BigBloomFilter>
{
    public void serialize(BigBloomFilter bf, DataOutputStream dos)
            throws IOException
    {
        dos.writeInt(bf.getHashCount());
        ObjectOutputStream oos = new ObjectOutputStream(dos);
        oos.writeObject(bf.bitset);
        oos.flush();
    }

    public BigBloomFilter deserialize(DataInputStream dis) throws IOException
    {
        int hashes = dis.readInt();
        ObjectInputStream ois = new ObjectInputStream(dis);
        try
        {
          OpenBitSet bs = (OpenBitSet) ois.readObject();
          return new BigBloomFilter(hashes, bs);
        } catch (ClassNotFoundException e)
        {
          throw new RuntimeException(e);
        }
    }
}


