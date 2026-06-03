"""Optional web chat front end.

A small FastAPI server that bridges a browser chat UI to the Kafka backend:
it publishes user messages to ``agent-requests`` and waits for the agent's
final reply on ``respond_to_user``, correlating by message id. This is a thin
client of the same backend the CLI uses; it adds no agent logic.
"""
