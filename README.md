# MongoDB Apollo Datasource

Datasources allow a means to streamline reading from a datasource.
See more details here: https://www.apollographql.com/docs/apollo-server/data/data-sources/

## A Datasource implementation for MongoDB

- Written in TypeScript! ☀️
- Includes caching and batching with DataLoader! 📦
- Compatible with MongoDB 🍃

### Usage

```typescript
class ProductsDataSource extends MongoDataSource<ProductDocument, Context> {

  // Methods from the data source
  async findAllProducts(filter: Filter<ProductDocument>) {
    return this.find(filter, {ttl: 60 /* 1 MINUTE*/}); //
  }
  
  async findProductById(id: ObjectID) {
    return this.findById(id);
  }
  
  async countProducts(filter: Filter<ProductDocument>) {
    return this.count(filter)
  }
  
  async createProduct(product: ProductDTO) {
    const result = await this.collection.create(product);
    
    // Clearing the cache of this record.
    await this.deleteById(product.id)
  }
}

```



