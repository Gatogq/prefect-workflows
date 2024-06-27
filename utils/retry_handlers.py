def visit_bot_retry_fn(task,task_run,state):
    
    try:
        state.result()

    except FileNotFoundError:

        return True
    
    except:
        
        return False