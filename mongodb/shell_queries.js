// Sample MongoDB shell queries.
// Run with:  docker exec -i auca-mongo mongosh ecommerce --quiet < mongodb/shell_queries.js
db = db.getSiblingDB("ecommerce");

print("--- collections and document counts");
db.getCollectionNames().sort().forEach(c => print(`  ${c}: ${db[c].countDocuments()}`));

print("--- user profile with the embedded purchase_summary and preferences (single _id read)");
printjson(db.users.findOne({_id: "user_000042"}));

print("--- complete journey of one converted session: entry, referrer, device, every page, cart");
db.sessions.aggregate([
  {$match: {conversion_status: "converted"}},
  {$limit: 1},
  {$project: {_id: 1, user_id: 1, referrer: 1, device: "$device.type", duration_seconds: 1,
              journey: {$map: {input: "$page_views", as: "p",
                               in: {$concat: ["$$p.page_type",
                                              {$cond: [{$ifNull: ["$$p.product_id", false]},
                                                       {$concat: [" (", "$$p.product_id", ")"]}, ""]}]}}},
              cart: 1}}
]).forEach(printjson);

print("--- latest order of that user (embedded line items; uses index user_id_1_timestamp_-1)");
printjson(db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).limit(1).toArray()[0]);

print("--- product with embedded price history and category reference");
printjson(db.products.findOne({_id: "prod_00123"}));

print("--- the same per-user order query WITHOUT the index (forced collection scan) vs WITH it");
const q = {user_id: "user_000042"};
const noIdx = db.transactions.find(q).sort({timestamp: -1}).hint({$natural: 1}).explain("executionStats").executionStats;
const withIdx = db.transactions.find(q).sort({timestamp: -1}).hint("user_id_1_timestamp_-1").explain("executionStats").executionStats;
print(`  without index: docs examined ${noIdx.totalDocsExamined}, returned ${noIdx.nReturned}, ${noIdx.executionTimeMillis} ms`);
print(`  with index   : docs examined ${withIdx.totalDocsExamined}, returned ${withIdx.nReturned}, ${withIdx.executionTimeMillis} ms`);

print("--- index use for the per-user order history query");
const plan = db.transactions.find({user_id: "user_000042"}).sort({timestamp: -1}).explain("executionStats");
print(`  winning plan stage: ${plan.queryPlanner.winningPlan.inputStage?.stage ?? plan.queryPlanner.winningPlan.stage}` +
      `, index: ${plan.queryPlanner.winningPlan.inputStage?.indexName ?? "-"}` +
      `, docs examined: ${plan.executionStats.totalDocsExamined}, returned: ${plan.executionStats.nReturned}` +
      `, time: ${plan.executionStats.executionTimeMillis} ms`);
