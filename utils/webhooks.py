from prefect.blocks.notifications import MicrosoftTeamsWebhook
from uuid import UUID
from datetime import datetime

teams_webhook = "teams-notifications-webhook"

webhook_block = MicrosoftTeamsWebhook.load(teams_webhook)

def success_hook(flow, flow_run, state):
        
    completion_time = datetime.now()

    webhook_block.notify(f"""Se ejecutó correctamente: {flow.name}. 
                               flujo concluido con status {state} en {completion_time}.
                               para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run.id}""")
    
def failure_hook(flow,flow_run,state):

    completion_time = datetime.now()

    webhook_block.notify(f"""Ocurrió un error al intentar ejecutar el flujo {flow.name}.
                                flujo concluido con status {state} en {completion_time}.
                                para ver los detalles de la ejecución: http://172.16.0.7:4200/flow-runs/flow-run/{flow_run.id}""")

    