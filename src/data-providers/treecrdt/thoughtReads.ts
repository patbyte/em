import { hexToBytes, nodeIdToBytes16 } from '@treecrdt/interface/ids'
import type { TreecrdtClient } from '@treecrdt/wa-sqlite'
import type Index from '../../@types/IndexType'
import type ThoughtId from '../../@types/ThoughtId'
import { ROOT_PARENT_ID } from '../../constants'
import { ATTRIBUTE_CHILDREN_TABLE, ensureAttributeChildrenSchema } from './attributeChildren'

const BATCH_SIZE = 500

type TreecrdtThoughtRowJson = {
  attributeValueByChildId: Index<string>
  childIds: ThoughtId[]
  id: ThoughtId
  parentId: ThoughtId | null
  payloadHex: string
  rank: number
}

export type TreecrdtThoughtRow = Omit<TreecrdtThoughtRowJson, 'parentId' | 'payloadHex'> & {
  parentId: ThoughtId
  payload: Uint8Array
}

/** Splits an array into bounded chunks to stay below SQLite's parameter limit. */
const chunk = <T>(values: T[], size: number): T[][] =>
  Array.from({ length: Math.ceil(values.length / size) }, (_, index) => values.slice(index * size, (index + 1) * size))

/** Creates the parameterized VALUES clause that preserves caller order and duplicate ids. */
const requestedValuesSql = (size: number): string =>
  Array.from({ length: size }, (_, index) => `(?${index + 1}, ${index})`).join(', ')

/**
 * Reads one bounded thought batch from TreeCRDT's materialized SQLite tables.
 * TODO: Move this query behind a TreeCRDT client batch API when one is available so em does not own the read schema boundary.
 */
const getThoughtRowsChunk = async (
  client: TreecrdtClient,
  ids: ThoughtId[],
): Promise<(TreecrdtThoughtRow | undefined)[]> => {
  const text = await client.runner.getText(
    `WITH requested(node, position) AS (VALUES ${requestedValuesSql(ids.length)})
     SELECT COALESCE(json_group_array(json(result)), '[]')
     FROM (
       SELECT requested.position,
         CASE WHEN thought.node IS NULL OR payload.payload IS NULL THEN NULL ELSE json_object(
           'id', lower(hex(thought.node)),
           'parentId', CASE WHEN thought.parent IS NULL THEN NULL ELSE lower(hex(thought.parent)) END,
           'payloadHex', lower(hex(payload.payload)),
           'rank', CASE WHEN thought.parent IS NULL OR thought.tombstone <> 0 THEN 0 ELSE (
             SELECT COUNT(*)
             FROM tree_nodes AS sibling
             WHERE sibling.parent = thought.parent
               AND sibling.tombstone = 0
               AND (
                 (sibling.order_key IS NULL AND thought.order_key IS NOT NULL)
                 OR (sibling.order_key < thought.order_key)
                 OR ((sibling.order_key = thought.order_key OR (sibling.order_key IS NULL AND thought.order_key IS NULL)) AND sibling.node < thought.node)
               )
           ) END,
           'childIds', json((
             SELECT COALESCE(json_group_array(child_id), '[]')
             FROM (
               SELECT lower(hex(child.node)) AS child_id
               FROM tree_nodes AS child
               WHERE child.parent = thought.node AND child.tombstone = 0
               ORDER BY child.order_key, child.node
             )
           )),
           'attributeValueByChildId', json((
             SELECT COALESCE(json_group_object(child_id, value), '{}')
             FROM ${ATTRIBUTE_CHILDREN_TABLE}
             WHERE parent_id = lower(hex(thought.node))
           ))
         ) END AS result
       FROM requested
       LEFT JOIN tree_nodes AS thought ON thought.node = requested.node
       LEFT JOIN tree_payload AS payload ON payload.node = thought.node
       ORDER BY requested.position
     )`,
    ids.map(nodeIdToBytes16),
  )

  if (!text) return ids.map(() => undefined)

  return (JSON.parse(text) as (TreecrdtThoughtRowJson | null)[]).map(row =>
    row
      ? {
          id: row.id,
          parentId: row.parentId ?? ROOT_PARENT_ID,
          payload: hexToBytes(row.payloadHex),
          rank: row.rank,
          childIds: row.childIds,
          attributeValueByChildId: row.attributeValueByChildId,
        }
      : undefined,
  )
}

/** Reads thoughts in bounded batches after advancing TreeCRDT materialization once. */
export const getTreecrdtThoughtRows = async (
  client: TreecrdtClient,
  ids: ThoughtId[],
): Promise<(TreecrdtThoughtRow | undefined)[]> => {
  if (ids.length === 0) return []

  await ensureAttributeChildrenSchema(client)
  // Use a public TreeCRDT read so pending materialization emits its normal change event before the raw read-model query.
  await client.tree.exists(ids[0])

  const rows = await Promise.all(chunk(ids, BATCH_SIZE).map(currentIds => getThoughtRowsChunk(client, currentIds)))
  return rows.flat()
}
