package simpledb.execution;
import simpledb.optimizer.LogicalJoinNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** A PlanCache is a helper class that can be used to store the best
 * way to order a given set of joins */
public class PlanCache {
    final Map<Set<LogicalJoinNode>,List<LogicalJoinNode>> bestOrders= new HashMap<>();
    final Map<Set<LogicalJoinNode>,Double> bestCosts= new HashMap<>();
    final Map<Set<LogicalJoinNode>,Integer> bestCardinalities = new HashMap<>();
    
    /** Add a new cost, cardinality and ordering for a particular join set.  Does not verify that the
        new cost is less than any previously added cost -- simply adds or replaces an existing plan for the
        specified join set
        @param s the set of joins for which a new ordering (plan) is being added
        @param cost the estimated cost of the specified plan
        @param card the estimatied cardinality of the specified plan
        @param order the ordering of the joins in the plan
    */
    public void addPlan(Set<LogicalJoinNode> s, double cost, int card, List<LogicalJoinNode> order) {
        bestOrders.put(s,order);                        
        bestCosts.put(s,cost);
        bestCardinalities.put(s,card);
    }
    
    /** Find the best join order in the cache for the specified plan 
        @param s the set of joins to look up the best order for
        @return the best order for s in the cache
    */
    public List<LogicalJoinNode> getOrder(Set<LogicalJoinNode> s) {
        return bestOrders.get(s);
    }
    
    /** Find the cost of the best join order in the cache for the specified plan 
        @param s the set of joins to look up the best cost for
        @return the cost of the best order for s in the cache
    */
    public double getCost(Set<LogicalJoinNode> s) {
        return bestCosts.get(s);
    }
    
    /** Find the cardinality of the best join order in the cache for the specified plan 
        @param s the set of joins to look up the best cardinality for
        @return the cardinality of the best order for s in the cache
    */
    public int getCard(Set<LogicalJoinNode> s) {
        return bestCardinalities.get(s);
    }
}
