# UnAI Discord Workspace

Official Discord Workspace for [UnAI](https://github.com/kasper-studios/UnAI).
Enables autonomous AI agents to interact with Discord servers, channels, direct messages, attachments, and members natively via Discord REST API v10.

## Installation

Install using the UnAI CLI:

```bash
unai workspace install discord
```

Enable the workspace:

```bash
unai workspace enable discord
```

## Features

- **Auth Session Management** (`discord.login`, `discord.logout`) per ADR-0004
- **Server & Channel Discovery** (`discord.servers.list`, `discord.channels.list`)
- **Messages & History** (`discord.messages.history`, `discord.messages.get`, `discord.messages.send`)
- **Replies & Attachments** (supports uploading local files, images, PDFs, attachments)
- **Members & Profiles** (`discord.members.list`, `discord.members.get`)
- **Notifications** (`discord.notifications.list`)
