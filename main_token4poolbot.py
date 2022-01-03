#!/usr/bin/env python3
import os
import json

import discord
from discord.ext import tasks
import sqlalchemy
import dotenv

client = discord.Client()
announcements = []
old_blocks = []
engine = None
newest_id_checked = 0

ART_PARTNERS = []

async def notify_all(message):
    for announcement in announcements:
        channel = announcement['channel']
        await channel.send(message)

def epoch_delegators(epoch, pool, ada_threshhold=1):
    statement = sqlalchemy.text('''select stake_address.view as stake_addr, amount, (select delegation.active_epoch_no from delegation where delegation.addr_id = stake_address.id and active_epoch_no <= epoch_no order by active_epoch_no desc limit 1) as joined_epoch from epoch_stake join stake_address on epoch_stake.addr_id=stake_address.id join pool_hash on epoch_stake.pool_id=pool_hash.id where pool_hash.view=:pool_view and epoch_no = :epoch and amount >= :threshold order by joined_epoch asc, stake_address.id asc''')

    with engine.connect() as con:
        results = con.execute(
            statement,
            pool_view=pool,
            epoch=epoch,
            threshold=ada_threshhold * 1000000,
        ).all()
    return results

def simple_raffle(num_entries, block_hash):
    hash_number = int(block_hash, 16)
    elected_number = hash_number % num_entries
    elected_number = elected_number + 1 # to convert from 0-index to line number

    return elected_number

def save_state():
    state = {
        'ART_PARTNERS':ART_PARTNERS,
        'old_blocks':old_blocks,
        'newest_id_checked':newest_id_checked,
    }
    with open(os.getenv('STATE_FILE'), 'w') as f_out:
        json.dump(state, f_out, indent=2)

def load_state():
    global ART_PARTNERS
    global old_blocks
    global newest_id_checked
    with open(os.getenv('STATE_FILE')) as f_in:
        state = json.load(f_in)
    ART_PARTNERS = state['ART_PARTNERS']
    old_blocks = state['old_blocks']
    newest_id_checked = state['newest_id_checked']


@client.event
async def on_ready():
    global newest_id_checked
    print("Token4Pool is ready!")
    for guild in client.guilds:
        print("Connected to:", guild)
        print("Channels:")
        for channel in guild.text_channels:
            print(channel)
            if all([
                'token4pool' in channel.name.lower(),
               ]):
                announcements.append({'guild':guild, 'channel':channel})
                print("Sending welcome message to", channel)
                await channel.send("Token4Pool Bot started!")
    print()
    if old_blocks:
        newest_id_checked = max(old_blocks)
    check_block.start()

@tasks.loop(minutes=15)
async def check_block():
    global newest_id_checked
    print("Checking for blocks..")
    statement = sqlalchemy.text('''select id from block order by id desc limit 1''')
    with engine.connect() as con:
        result = con.execute(
            statement,
        ).first()
    newest_id = result.id
    statement = sqlalchemy.text('''select * from pool_hash join slot_leader on slot_leader.pool_hash_id = pool_hash.id join block on block.slot_leader_id = slot_leader.id where pool_hash.view = :pool_view and block.id > :id_threshold order by block.id asc;''')

    with engine.connect() as con:
        results = con.execute(
            statement,
            pool_view=os.getenv("POOL_VIEW"),
            id_threshold=newest_id_checked,
        ).all()
    newest_id_checked = newest_id

    for result in results:
        if result.id in old_blocks:
            continue

        block_hash = result.hash.hex()
        epoch = result.epoch_no
        block_time = result.time
        delegator_list = epoch_delegators(pool=os.getenv("POOL_VIEW"), epoch=epoch)
        winner_number = simple_raffle(len(delegator_list), block_hash)
        winner_delegator = delegator_list[winner_number-1]
        art_winner_number = simple_raffle(len(ART_PARTNERS), block_hash)
        winner_artist = ART_PARTNERS[art_winner_number-1]
        # RMKeyView(['stake_addr', 'amount', 'joined_epoch'])
        message_text = f':tada: CKEYS minted a new block :tada:'
        message_text += f'\n<https://explorer.cardano.org/en/block?id={block_hash}>'
        message_text += f'\nEpoch {epoch} at {block_time} UTC'
        message_text += f'\n\n:art: Token4Pool Art: {winner_artist}'
        message_text += f'\n:point_right: Winner: <https://pool.pm/{winner_delegator["stake_addr"]}>'
        message_text += f'\n\nStats:'
        message_text += f'\n\t- :clock1: Stayed with our pool since epoch {winner_delegator["joined_epoch"]}'
        message_text += f'\n\t- :moneybag: Delegated a total of {winner_delegator["amount"]/1000000:.2f}ADA in epoch {epoch}'
        message_text += f'\n\t- :four_leaf_clover: Winner number: {winner_number}'
        message_text += f'\n\t- :four_leaf_clover: Winner art number: {art_winner_number}'
        message_text += f'\n\t- :stadium: Total Delegators in Epoch {epoch}: {len(delegator_list)}'
        message_text += f'\n\n- :notepad_spiral: List of eligible partner projects: <https://pastebin.com/EEwDnvV2>'
        message_text += f'\n\n\nMade with :green_heart: by [the Minister]#0001'
        message_text += f'\n<https://github.com/the-Minister-0001/token4pool-bot>'
        await notify_all(message_text)
        print(f"Block ID {result.id} minted in epoch {epoch} at {block_time} found")
        old_blocks.append(result.id)
    save_state()


def main():
    global engine
    dotenv.load_dotenv()
    load_state()
    engine = sqlalchemy.create_engine(os.getenv('DBSYNC_CONNECTION_STRING'))
    client.run(os.getenv('DISCORD_TOKEN'))


if __name__ == '__main__':
    main()
