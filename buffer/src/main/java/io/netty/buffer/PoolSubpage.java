/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

final class PoolSubpage<T> implements PoolSubpageMetric {

    final PoolChunk<T> chunk;

    /**
     * 在 {@link PoolChunk# memoryMap} 的节点编号
     */
    private final int memoryMapIdx;

    /**
     * 在 Chunk 中，偏移字节量
     *
     * @see PoolChunk# runOffset(int)
     */
    private final int runOffset;
    private final int pageSize;

    /**
     * Subpage 分配信息数组
     *
     * 每个 long 的 bits 位代表一个 Subpage 是否分配。
     * 因为 PoolSubpage 可能会超过 64 个( long 的 bits 位数 )，所以使用数组。
     *   例如：Page 默认大小为 8KB ，Subpage 默认最小为 16 B ，所以一个 Page 最多可包含 8 * 1024 / 16 = 512 个 Subpage 。
     *        因此，bitmap 数组大小为 512 / 64 = 8 。
     * 另外，bitmap 的数组大小，使用 {@link #bitmapLength} 来标记。或者说，bitmap 数组，默认按照 Subpage 的大小为 16B 来初始化。
     *    为什么是这样的设定呢？因为 PoolSubpage 可重用，通过 {@link #init(PoolSubpage, int)} 进行重新初始化。
     */
    private final long[] bitmap;

    /**
     * 双向链表构造
     */
    PoolSubpage<T> prev;
    PoolSubpage<T> next;

    /**
     * 是否未销毁
     */
    boolean doNotDestroy;

    /**
     * 每个 Subpage 的占用内存大小
     */
    int elemSize;

    /**
     * 总共 Subpage 的数量
     */
    private int maxNumElems;

    /**
     * {@link #bitmap} 长度
     */
    private int bitmapLength;

    /**
     * 下一个可分配 Subpage 的数组位置
     */
    private int nextAvail;

    /**
     * 剩余可用 Subpage 的数量
     */
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /** Special constructor that creates a linked list head */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize;
        // 默认按最小16b分配。所以除以 2^4 * 2^6,2^4为最小分配单元(16)，2^6为long类型字节数(64)。
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64
        // 初始化
        init(head, elemSize);
    }

    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        this.elemSize = elemSize;
        if (elemSize != 0) {
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;
            // 2^6，long类型的bit位，计算出需要的bitmap长度
            bitmapLength = maxNumElems >>> 6;
            // 有余数，多加一位
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;
            }

            for (int i = 0; i < bitmapLength; i ++) {
                // 重置，有可能是复用，当前数组元素数据不为0.
                bitmap[i] = 0;
            }
        }
        addToPool(head);
    }

    /**
     * Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        // 防御性编程，不存在这种情况。
        if (elemSize == 0) {
            return toHandle(0);
        }

        // 分配完或已被销毁，返回-1.
        if (numAvail == 0 || !doNotDestroy) {
            return -1;
        }

        // 获得下一个可用的 Subpage 在 bitmap 中的总体位置
        final int bitmapIdx = getNextAvail();

        // 除数定位在bitmap中的位置，余数定位在long数据中的bit位。
        // 获得下一个可用的 Subpage 在 bitmap 中数组的位置
        int q = bitmapIdx >>> 6;
        // 获得下一个可用的 Subpage 在 bitmap 中数组的位置的第几 bits
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) == 0;
        // 修改 Subpage 在 bitmap 中不可分配。对应bit为更新为1。
        bitmap[q] |= 1L << r;

        // 可用 Subpage 内存块的计数减一
        if (-- numAvail == 0) {
            // 无可用 Subpage 内存块，从双向链表中移除。
            removeFromPool();
        }

        // 计算 handle
        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            return true;
        }
        int q = bitmapIdx >>> 6;
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        // 对应bit位值更新为0
        bitmap[q] ^= 1L << r;

        // 设置下一个可用为当前 Subpage
        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            // 有可用的，添加到pool中
            addToPool(head);
            return true;
        }

        // 还有 Subpage 在被使用
        if (numAvail != maxNumElems) {
            return true;
        } else {
            // 双向链表中，只有该节点，不进行移除
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }
            // 没有 subpage 在被使用了，释放掉

            // 标记为已销毁
            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            // 从双向链表中移除
            removeFromPool();
            return false;
        }
    }

    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        // 插入到 head 和 head.next 之间
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        // 前后节点之间，移除自己。
        prev.next = next;
        next.prev = prev;
        // 当前节点，置空
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        // <1> nextAvail 大于 0 ，意味着已经“缓存”好下一个可用的位置，直接返回即可。
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }
        return findNextAvail();
    }

    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            // 不为0，表示 bits 中存在为0的bit。
            if (~bits != 0) {
                // 寻找bit为0的
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        // 计算基础值，表示在 bitmap 的数组下标
        final int baseVal = i << 6;

        // 遍历 64 bits
        for (int j = 0; j < 64; j ++) {
            if ((bits & 1) == 0) {
                // 可能 bitmap 最后一个元素，并没有 64 位，通过 baseVal | j < maxNumElems 来保证不超过上限。
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            bits >>>= 1;
        }
        // 未找到
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        // 低 32 bits ：memoryMapIdx。高 32 bits ：bitmapIdx。
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        synchronized (chunk.arena) {
            if (!this.doNotDestroy) {
                doNotDestroy = false;
                // Not used for creating the String.
                maxNumElems = numAvail = elemSize = -1;
            } else {
                doNotDestroy = true;
                maxNumElems = this.maxNumElems;
                numAvail = this.numAvail;
                elemSize = this.elemSize;
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
