package com.tr.ap.es.join;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.script.ScriptModule;

public class JoinPlugin extends AbstractPlugin
{
    public JoinPlugin()
    {
    }

    public String name()
    {
        return "join";
    }

    public String description()
    {
        return "Various actions to join documents accross indices";
    }

    public void onModule(ScriptModule module)
    {
        module.registerScript("left_join_single", LeftJoinSingle.Factory.class);
    }

}
