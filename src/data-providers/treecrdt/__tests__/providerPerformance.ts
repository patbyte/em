import type { TreecrdtClient } from '@treecrdt/wa-sqlite'
import type Index from '../../../@types/IndexType'
import type Thought from '../../../@types/Thought'
import type ThoughtId from '../../../@types/ThoughtId'
import type Timestamp from '../../../@types/Timestamp'
import { EM_TOKEN, GLOBAL_ROOT_TOKEN } from '../../../constants'
import treecrdtThoughtspace, { init as initTreecrdtThoughtspace } from '../thoughtspace'
import { getTreecrdtClient, initTreecrdt } from '../treecrdt'

const TEST_REPLICA_ID = new Uint8Array(32).fill(2)
const FIXTURE_SIZE = 8

type ThoughtFixture = {
  movePlacements: Index<ThoughtId | null>
  readIds: ThoughtId[]
  thoughts: Thought[]
}

/** Converts a numeric fixture index to a valid TreeCRDT thought id. */
const fixtureId = (index: number): ThoughtId => index.toString(16).padStart(32, '0') as ThoughtId

/** Creates a minimal thought for provider performance fixtures. */
const thought = (id: ThoughtId, parentId: ThoughtId, value: string, rank: number): Thought => ({
  id,
  parentId,
  value,
  rank,
  childrenMap: {},
  created: 1 as Timestamp,
  lastUpdated: 1 as Timestamp,
  updatedBy: 'performance-fixture',
})

/** Creates siblings under one parent to expose costs that scale with tree width. */
const wideFixture = (size: number): ThoughtFixture => {
  const parentId = fixtureId(1000)
  const parent = thought(parentId, EM_TOKEN, 'wide-parent', 0)
  const children = Array.from({ length: size }, (_, index) =>
    thought(fixtureId(1001 + index), parentId, `wide-${index}`, index),
  )

  return {
    thoughts: [parent, ...children],
    readIds: children.map(child => child.id),
    movePlacements: Object.fromEntries(
      children.map((child, index) => [child.id, index === 0 ? null : children[index - 1].id]),
    ),
  }
}

/** Creates a single-child chain to expose costs that scale with tree depth. */
const deepFixture = (size: number): ThoughtFixture => {
  const thoughts = Array.from({ length: size }, (_, index) => {
    const id = fixtureId(2000 + index)
    const parentId = index === 0 ? EM_TOKEN : fixtureId(2000 + index - 1)
    return thought(id, parentId, `deep-${index}`, 0)
  })

  return {
    thoughts,
    readIds: thoughts.map(current => current.id),
    movePlacements: Object.fromEntries(thoughts.map(current => [current.id, null])),
  }
}

/** Persists a performance fixture through the real TreeCRDT data provider. */
const persistFixture = async ({ movePlacements, thoughts }: ThoughtFixture): Promise<void> => {
  await treecrdtThoughtspace.updateThoughts({
    thoughtIndexUpdates: Object.fromEntries(thoughts.map(current => [current.id, current])),
    lexemeIndexUpdates: {},
    lexemeIndexUpdatesOld: {},
    schemaVersion: 0,
    movePlacements,
  })
}

/** Instruments TreeCRDT methods that cross the worker or SQLite boundary during batched reads. */
const instrumentTreecrdtReads = (client: TreecrdtClient) => ({
  children: vi.spyOn(client.tree, 'children'),
  exists: vi.spyOn(client.tree, 'exists'),
  getPayload: vi.spyOn(client.tree, 'getPayload'),
  parent: vi.spyOn(client.tree, 'parent'),
  sqlGetText: vi.spyOn(client.runner, 'getText'),
})

/** Returns stable call counts from the TreeCRDT read instrumentation. */
const readCallCounts = (spies: ReturnType<typeof instrumentTreecrdtReads>) => ({
  children: spies.children.mock.calls.length,
  exists: spies.exists.mock.calls.length,
  getPayload: spies.getPayload.mock.calls.length,
  parent: spies.parent.mock.calls.length,
  sqlGetText: spies.sqlGetText.mock.calls.length,
})

beforeEach(async () => {
  await treecrdtThoughtspace.clear()
  await initTreecrdt({ storage: 'memory', runtime: 'direct' })
  await initTreecrdtThoughtspace(TEST_REPLICA_ID)
})

afterEach(async () => {
  vi.restoreAllMocks()
  await treecrdtThoughtspace.clear()
})

it.each([
  ['wide', wideFixture],
  ['deep', deepFixture],
] as const)('batches TreeCRDT provider reads for a %s fixture', async (_name, createFixture) => {
  const fixture = createFixture(FIXTURE_SIZE)
  await persistFixture(fixture)
  const readSpies = instrumentTreecrdtReads(getTreecrdtClient())

  const result = await treecrdtThoughtspace.getThoughtsByIds(fixture.readIds)

  expect(result.map(current => current?.id)).toEqual(fixture.readIds)
  expect(readCallCounts(readSpies)).toEqual({
    children: 0,
    exists: 1,
    getPayload: 0,
    parent: 0,
    sqlGetText: 1,
  })
})

it('preserves single-read semantics, caller order, duplicates, and missing thoughts', async () => {
  const parent = thought(fixtureId(3000), EM_TOKEN, 'parent', 0)
  const child = thought(fixtureId(3001), parent.id, 'child', 0)
  const archived = { ...thought(fixtureId(3002), parent.id, 'archived', 1), archived: 2 as Timestamp }
  const attribute = thought(fixtureId(3003), child.id, '=pin', 0)
  const thoughts = [parent, child, archived, attribute]

  await persistFixture({
    thoughts,
    readIds: thoughts.map(current => current.id),
    movePlacements: {
      [parent.id]: null,
      [child.id]: null,
      [archived.id]: child.id,
      [attribute.id]: null,
    },
  })

  const missingId = fixtureId(3999)
  const readIds = [archived.id, missingId, child.id, archived.id, GLOBAL_ROOT_TOKEN]
  const expected = await Promise.all(readIds.map(current => treecrdtThoughtspace.getThoughtById(current)))

  await expect(treecrdtThoughtspace.getThoughtsByIds(readIds)).resolves.toEqual(expected)
})

it('avoids TreeCRDT boundary calls for an empty batch', async () => {
  const readSpies = instrumentTreecrdtReads(getTreecrdtClient())

  await expect(treecrdtThoughtspace.getThoughtsByIds([])).resolves.toEqual([])
  expect(readCallCounts(readSpies)).toEqual({
    children: 0,
    exists: 0,
    getPayload: 0,
    parent: 0,
    sqlGetText: 0,
  })
})

it('preserves single-read semantics for deleted thoughts', async () => {
  const parent = thought(fixtureId(4000), EM_TOKEN, 'parent', 0)
  const child = thought(fixtureId(4001), parent.id, 'child', 0)
  await persistFixture({
    thoughts: [parent, child],
    readIds: [child.id],
    movePlacements: { [parent.id]: null, [child.id]: null },
  })
  await treecrdtThoughtspace.updateThoughts({
    thoughtIndexUpdates: { [child.id]: null },
    lexemeIndexUpdates: {},
    lexemeIndexUpdatesOld: {},
    schemaVersion: 0,
  })

  const expected = await treecrdtThoughtspace.getThoughtById(child.id)
  await expect(treecrdtThoughtspace.getThoughtsByIds([child.id])).resolves.toEqual([expected])
})

it('preserves order across bounded SQLite batches', async () => {
  const current = thought(fixtureId(5000), EM_TOKEN, 'repeated', 0)
  await persistFixture({ thoughts: [current], readIds: [current.id], movePlacements: { [current.id]: null } })
  const missingId = fixtureId(5001)
  const readIds = Array.from({ length: 501 }, (_, index) => (index % 2 === 0 ? current.id : missingId))
  const readSpies = instrumentTreecrdtReads(getTreecrdtClient())

  const result = await treecrdtThoughtspace.getThoughtsByIds(readIds)

  expect(result.map(thought => thought?.id)).toEqual(readIds.map(id => (id === current.id ? id : undefined)))
  expect(readCallCounts(readSpies)).toEqual({
    children: 0,
    exists: 1,
    getPayload: 0,
    parent: 0,
    sqlGetText: 2,
  })
})
