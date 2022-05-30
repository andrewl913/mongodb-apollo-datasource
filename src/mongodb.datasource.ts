import { DataSource, DataSourceConfig } from "apollo-datasource";
import { Collection, ObjectId, WithId, Filter } from "mongodb";
import { KeyValueCache, KeyValueCacheSetOptions } from "apollo-server-caching";
import DataLoader from "dataloader";
import { EJSON } from "bson";
import crypto from "crypto";
import sift from "sift";
import sortKeys from "sort-keys";

export type CachedOptions = {
  ttl: number;
};

type CountFacet = Record<string, Array<{ count: number }>>;

export abstract class MongoDataSource<
  TDocument extends { _id: ObjectId | string },
  TContext
> extends DataSource<TContext> {
  private CACHE_PREFIX!: string;
  private cache?: KeyValueCache<string>;

  private findByIdLoader?: DataLoader<
    ObjectId | string,
    WithId<TDocument> | null
  >;
  private findLoader?: DataLoader<
    Filter<WithId<TDocument>>,
    WithId<TDocument>[],
    string
  >;
  private countLoader?: DataLoader<Filter<WithId<TDocument>>, number, string>;
  protected context?: TContext;

  constructor(protected readonly collection: Collection<TDocument>) {
    super();
    this.CACHE_PREFIX = `mongo-${collection.collectionName}-`;
  }

  initialize(config: DataSourceConfig<TContext>): void | Promise<void> {
    this.cache = config.cache;
    this.context = config.context;

    this.findByIdLoader = new DataLoader<
      ObjectId | string,
      WithId<TDocument> | null
    >(async (keys: ReadonlyArray<string | ObjectId>) => {
      const items = await this.collection
        .find({ _id: { $in: [...keys] } } as unknown as Filter<TDocument>)
        .toArray();

      const result = keys.map(
        (key) =>
          items.find((i) => {
            // its a string id
            if (typeof i._id === "string" && typeof key === "string") {
              return i._id === key;
            }
            // its an object id
            if (typeof i._id !== "string" && typeof key !== "string") {
              return i._id.equals(key);
            }

            // we don't use number ids around here so we don't match
            return false;
          }) ?? null
      );
      return result;
    });

    this.findLoader = new DataLoader<
      Filter<WithId<TDocument>>,
      WithId<TDocument>[],
      string
    >(
      async (queries: ReadonlyArray<Filter<WithId<TDocument>>>) => {
        const commonQuery = queries.reduce(
          (acc, query: Record<string, unknown>) => {
            const commonProps = Object.keys(query)
              .filter((key) =>
                queries.every(
                  (q: Record<string, unknown>) =>
                    EJSON.stringify(q[key] as EJSON.SerializableTypes) ===
                    EJSON.stringify(query[key] as EJSON.SerializableTypes)
                )
              )
              .reduce((acc, curr) => ({ ...acc, [curr]: query[curr] }), {});
            return {
              ...acc,
              ...commonProps,
            };
          },
          {} as Record<string, unknown>
        );

        const result = await this.collection
          .find({
            ...commonQuery,
            $or: [...queries],
          })
          .toArray();

        return queries.map((query) => {
          return result.filter(sift(query as any)) ?? [];
        });
      },
      {
        // Allows us to use the provided params to take advantage of caching
        cacheKeyFn: (param) => EJSON.stringify(sortKeys(param, { deep: true })),
      }
    );

    this.countLoader = new DataLoader<
      Filter<WithId<TDocument>>,
      number,
      string
    >(
      async (queries: ReadonlyArray<Filter<WithId<TDocument>>>) => {
        const facets = queries.reduce((obj, query) => {
          const facetKey = crypto
            .createHash("md5")
            .update(EJSON.stringify(sortKeys(query, { deep: true })))
            .digest("base64");

          return {
            ...obj,
            [facetKey]: [{ $match: query }, { $count: "count" }],
          };
        }, {});

        const result = (
          await this.collection
            .aggregate([
              {
                $facet: {
                  ...facets,
                },
              },
            ])
            .toArray()
        )?.[0] as CountFacet;

        return queries.map((query) => {
          const facetKey = crypto
            .createHash("md5")
            .update(EJSON.stringify(sortKeys(query, { deep: true })))
            .digest("base64");

          return result[facetKey]?.[0]?.count ?? 0;
        });
      },
      {
        // Allows us to use the provided params to take advantage of caching
        cacheKeyFn: (param) => EJSON.stringify(sortKeys(param, { deep: true })),
      }
    );
  }

  /**
   *  Finds many documents given a filter.
   * @param filter Mongodb filter
   * @param options Cached options
   * @returns Array of documents.
   */
  async find(filter: Filter<WithId<TDocument>>, options?: CachedOptions) {
    const key = this.cacheKey(filter, "find");
    const items = await this.cache?.get(key);

    if (items) {
      return EJSON.parse(items) as WithId<TDocument>[];
    }

    const documents = await this.findLoader?.load(filter);
    if (!documents || documents.length === 0) {
      return [];
    }

    await this.cache?.set(key, EJSON.stringify(documents), options);

    return documents;
  }

  /**
   *  Finds one document given a filter.
   * @param filter Mongodb filter
   * @param options Cached options
   * @returns A single document.
   */
  public async findOne(
    filter: Filter<WithId<TDocument>>,
    options?: CachedOptions
  ) {
    const documents = await this.find(filter, options);
    return documents?.[0] ?? null;
  }

  /**
   *  Finds one document by its id.
   * @param id MongoDB ObjectId or string id.
   * @param options Cached options
   * @returns A single document.
   */
  public async findById(
    id: ObjectId | string,
    options?: KeyValueCacheSetOptions
  ) {
    const key = this.cacheKey(id, "findById");
    const item = await this.cache?.get(key);

    if (item) {
      return EJSON.parse(item) as TDocument;
    }

    const document = await this.findByIdLoader?.load(id);

    if (!document) {
      return null;
    }

    this.cache?.set(key, EJSON.stringify(document), options);

    return document;
  }

  /**
   * Counts the number of documents that match the filter.
   * @param filter
   * @param options
   * @returns A number.
   */
  public async count(
    filter: Filter<WithId<TDocument>>,
    options?: CachedOptions
  ) {
    const key = this.cacheKey(filter, "count");

    const count = await this.cache?.get(key);

    if (typeof count === "string") {
      return +count;
    }

    const countResult = await this.countLoader?.load(filter);

    if (typeof countResult !== "number") {
      return 0;
    }

    await this.cache?.set(key, countResult.toString(), options);

    return countResult;
  }

  /**
   * Removes a single document from the cache.
   * @param id ObjcetId or string id
   */
  public async deleteFromCacheById(id: ObjectId | string) {
    const key = this.cacheKey(id, "findById");
    this.findByIdLoader?.clear(id);

    await this.cache?.delete(key);
  }

  /**
   * Removes documents from the cache that match the filter.
   * @param filter A mongodb filter
   */
  public async deleteFromCache(filter: Filter<WithId<TDocument>>) {
    const findKey = this.cacheKey(filter, "find");
    const countKey = this.cacheKey(filter, "find");
    this.findLoader?.clear(filter);
    this.countLoader?.clear(filter);
    await Promise.all([
      this.cache?.delete(findKey),
      this.cache?.delete(countKey),
    ]);
  }

  private isObjectId(id: unknown): id is ObjectId {
    return id instanceof ObjectId;
  }

  /**
   *
   * @param id MongoDB ObjectId or string id.
   * @param operationName the name of the operation to cache
   */
  protected cacheKey(id: ObjectId | string, operationName?: keyof this): string;
  /**
   *
   * @param filter A mongodb filter
   * @param operationName The name of the operation to cache
   */
  protected cacheKey(
    filter: Filter<WithId<TDocument>>,
    operationName?: keyof this
  ): string;
  protected cacheKey(
    filterOrId: Filter<WithId<TDocument>> | string | ObjectId,
    operationName?: keyof this
  ): string {
    const postFix =
      typeof filterOrId === "string"
        ? filterOrId
        : this.isObjectId(filterOrId)
        ? filterOrId.toHexString()
        : EJSON.stringify(sortKeys(filterOrId, { deep: true }));

    const key = crypto
      .createHash("md5")
      .update(`${this.CACHE_PREFIX}${String(operationName ?? "")}${postFix}`)
      .digest("base64");

    return key;
  }
}
