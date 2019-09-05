package com.yahoo.search.searchers;

import com.yahoo.search.Query;
import com.yahoo.search.Result;
import com.yahoo.search.Searcher;
import com.yahoo.search.searchchain.Execution;
import com.yahoo.component.chain.dependencies.Provides;
import com.yahoo.component.chain.dependencies.After;
import com.yahoo.search.searchchain.PhaseNames;

/**
 * @author baldersheim
 */
@Provides(PhaseNames.BACKEND)
@After("*")
public class DummyBackend extends Searcher {

    @Override
    public Result search(Query query, Execution execution) {
        Result empty = new Result(query);
        empty.setTotalHitCount(1);
        return empty;
    }

}
