package ibis.maestro;


/**
 * Information about all tasks.
 *
 * @author Kees van Reeuwijk.
 */
public class TaskInfoList
{
    private final TaskInfo taskInfoList[];

    TaskInfoList( TaskType[] taskTypes )
    {
        taskInfoList = new TaskInfo[Globals.numberOfTaskTypes];
        for( TaskType type: taskTypes ) {
            taskInfoList[type.index] = new TaskInfo( type );
        }
    }
    
    TaskInfo getTaskInfo( TaskType type )
    {
        return taskInfoList[type.index];
    }

    void registerNode( NodeInfo nodeInfo )
    {
        for( TaskInfo info: taskInfoList )
        {
            if( info != null ) {
                info.registerNode( nodeInfo );
            }
        }
    }
}
