package org.apache.ignite.cache.query.index.inline;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexSearchRow;
import org.apache.ignite.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.cache.query.index.inline.io.InlinedIndexRow;
import org.apache.ignite.cache.query.index.sorted.SortedIndexFunction;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRowAdapter;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.IOVersions;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.reuse.ReuseList;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageLockListener;
import org.apache.ignite.internal.processors.cache.tree.CacheDataRowStore;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.ignite.cache.query.index.inline.keys.NullableInlineIndexKey.CANT_BE_COMPARE;

/**
 * BPlusTree where nodes stores inlined index keys.
 */
public class InlinedIndexTree extends BPlusTree<InlinedIndexSearchRow, InlinedIndexRow> {

    private final InlineIndexKeyRegistry registry = new InlineIndexKeyRegistry();

    private final int idxKeysSize;

    private final InlineIndexKey[] schema;

    private final CacheGroupContext grp;

    private final SortedIndexFunction[] idxFuncs;

    /**
     * Constructor.
     */
    public InlinedIndexTree(
        SortedIndexDefinition def,
        CacheGroupContext grp, String name,
        ReuseList reuseList,
        CacheDataRowStore rowStore, long metaPageId, boolean initNew,
        PageLockListener lockLsnr,
        int configuredInlineSize) throws IgniteCheckedException {
        super(
            name,
            grp.groupId(),
            grp.name(),
            grp.dataRegion().pageMemory(),
            grp.dataRegion().config().isPersistenceEnabled() ? grp.shared().wal() : null,
            grp.offheap().globalRemoveId(),
            metaPageId,
            reuseList,
            grp.shared().kernalContext().failure(),
            lockLsnr
        );

        schema = new InlineIndexKey[def.getIdxFuncs().size()];
        for (int i = 0; i < def.getIdxFuncs().size(); ++i) {
            Class<?> idxKeyCls = def.getIdxFuncs().get(i).getIdxKeyCls();
            schema[i] = registry.get(idxKeyCls);
        }

        idxFuncs = def.getIdxFuncs().toArray(new SortedIndexFunction[]{});

        // TODO: check computeInlineSize todos
        idxKeysSize = computeInlineSize(schema, configuredInlineSize, grp.config().getSqlIndexMaxInlineSize());

        setIos(
            // -1 is required as payload starts with 1, and indexes in list of IOs are with 0.
            (IOVersions<BPlusInnerIO<InlinedIndexSearchRow>>) PageIO.getInnerVersions(idxKeysSize - 1, false),
            (IOVersions<BPlusLeafIO<InlinedIndexSearchRow>>)PageIO.getLeafVersions(idxKeysSize - 1, false));

        initTree(true, idxKeysSize);

        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override protected int compare(BPlusIO<InlinedIndexSearchRow> io, long pageAddr, int idx,
        InlinedIndexSearchRow row) throws IgniteCheckedException {

        if ((schema.length != row.idxKeys().length) && row.isFullSchemaSearch())
            throw new IgniteCheckedException("Find is configured for full schema search.");

        int fieldOff = 0;

        // Use it when can't compare values (variable length, for example).
        int lastIdxUsed = row.idxKeys().length;

        // TODO: compare keys.length with row.idxKeys
        for (int i = 0; i < row.idxKeys().length; i++) {
            try {
                int maxSize = idxKeysSize - fieldOff;

                int off = io.offset(idx);

                int cmp = schema[i].compare(pageAddr, off + fieldOff, maxSize, row.idxKeys()[i]);

                fieldOff += schema[i].size();

                if (cmp == CANT_BE_COMPARE) {
                    lastIdxUsed = i;
                    break;
                }

                if (cmp != 0)
                    return cmp;

            } catch (Exception e) {
                throw new IgniteException("Failed to store new index row.", e);
            }
        }

        if (lastIdxUsed < row.idxKeys().length) {
            InlinedIndexRow currRow = getRow(io, pageAddr, idx);

            for (int i = lastIdxUsed; i < row.idxKeys().length; i++) {
                Comparable v1 = (Comparable) currRow.idxKeys()[i];
                Comparable v2 = (Comparable) row.idxKeys()[i];

                int c = v1.compareTo(v2);

                if (c != 0)
                    return c;
            }
        }

        // TODO: ordering

        return 0;
    }

    /** {@inheritDoc} */
    @Override public InlinedIndexRow getRow(BPlusIO<InlinedIndexSearchRow> io, long pageAddr, int idx, Object ignore)
        throws IgniteCheckedException {

        int off = io.offset(idx);

        long link = PageUtils.getLong(pageAddr, off + idxKeysSize);

        CacheDataRowAdapter row = new CacheDataRowAdapter(link);

        // TODO: get from row cache
        row.initFromLink(
            grp,
            CacheDataRowAdapter.RowData.FULL,
            true
        );

        CacheObjectContext ctx = grp.cacheObjectContext();

        Object cacheKey = row.key().value(ctx, false);
        Object cacheVal = row.value().value(ctx, false);

        Object[] idxKeys = new Object[idxFuncs.length];

        for (int i = 0; i < idxFuncs.length; ++i)
            idxKeys[i] = idxFuncs[i].apply(cacheKey, cacheVal);

        return new InlinedIndexRow(idxKeys, null, row);
    }

    /**
     * @param inlineIdxs Inline index helpers.
     * @param cfgInlineSize Inline size from index config. // TODO, it ignores cache.setMaxSqlInlineIndex?
     * @param maxInlineSize Max inline size from cache config. // TODO
     * @return Inline size.
     */
    protected static int computeInlineSize(
        InlineIndexKey[] inlineIdxs,
        int cfgInlineSize,
        int maxInlineSize
    ) {
        if (cfgInlineSize == 0)
            return 0;

        if (F.isEmpty(inlineIdxs))
            return 0;

        if (cfgInlineSize != -1)
            return Math.min(PageIO.MAX_PAYLOAD_SIZE, cfgInlineSize);

        int propSize = maxInlineSize == -1
            ? IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE, IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT)
            : maxInlineSize;

        int size = 0;

        for (InlineIndexKey idxHelper : inlineIdxs) {
            if (idxHelper.size() <= 0) {
                size = propSize;
                break;
            }

            // 1 byte type + size
            size += idxHelper.size() + 1;
        }

        return Math.min(PageIO.MAX_PAYLOAD_SIZE, size);
    }

    /** Default value for {@code IGNITE_MAX_INDEX_PAYLOAD_SIZE} */
    public static final int IGNITE_MAX_INDEX_PAYLOAD_SIZE_DEFAULT = 10;
}
