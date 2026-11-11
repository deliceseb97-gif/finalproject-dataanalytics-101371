// Sample MongoDB shell queries.
// Run with:  docker exec -i auca-mongo mongosh ecommerce --quiet < mongodb/shell_queries.js
db = db.getSiblingDB("ecommerce");

print("--- collections and document counts");
db.getCollectionNames().sort().forEach(c => print(`  ${c}: ${db[c].countDocuments()}`));

print("--- user profile with the materialised purchase_summary (computed pattern, single _id read)");
printjson(db.users.findOne({_id: "user_000042"}));

print("--- latest order of that user (embedded line items; uses index user_id_1_timestamp_-1)");
printjson(db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).limit(1).toArray()[0]);

print("--- product with embedded price history and category reference");
printjson(db.products.findOne({_id: "prod_00123"}));

print("--- index use for the per-user order history query");
const plan = db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).explain("executionStats");
print(`  winning plan stage: ${plan.queryPlanner.winningPlan.inputStage?.stage ?? plan.queryPlanner.winningPlan.stage}` +
      `, index: ${plan.queryPlanner.winningPlan.inputStage?.indexName ?? "-"}` +
      `, docs examined: ${plan.executionStats.totalDocsExamined}, returned: ${plan.executionStats.nReturned}` +
      `, time: ${plan.executionStats.executionTimeMillis} ms`);
