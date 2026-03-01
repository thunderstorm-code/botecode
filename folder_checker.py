import logging
import re
import asyncio
from typing import List, Dict, Any, Optional
from telethon import TelegramClient
from telethon.tl import functions
from telethon.tl import types as tl_types
from telethon.tl.types import InputPeerChannel, InputPeerChat, Channel, Chat, InputPeerSelf
from telethon import errors

logger = logging.getLogger(__name__)

RE_FOLDER_SLUG = re.compile(r"^(https?://)?t\.me/addlist/([A-Za-zA-Z0-9_-]+)$")


def extract_folder_slug(link: str) -> Optional[str]:
    match = RE_FOLDER_SLUG.search(link.strip())
    return match.group(2) if match else None


def is_folder_link(link: str) -> bool:
    return RE_FOLDER_SLUG.search(link.strip()) is not None


async def import_folder_and_get_chats(client: TelegramClient, slug: str) -> Dict[str, Any]:
    try:
        check = await client(functions.chatlists.CheckChatlistInviteRequest(slug))

        if isinstance(check, tl_types.chatlists.ChatlistInviteAlready):
            # Уже импортирована ранее
            filter_id = check.filter_id
            filters = await client(functions.messages.GetDialogFiltersRequest())
            target_filter = None
            for f in filters.filters:
                if hasattr(f, 'id') and f.id == filter_id:
                    target_filter = f
                    break

            chats_data = []
            if target_filter and hasattr(target_filter, 'include_peers'):
                for peer in target_filter.include_peers:
                    try:
                        entity = await client.get_entity(peer)
                        if entity:
                            chats_data.append({
                                "id": entity.id,
                                "title": str(getattr(entity, 'title', 'Unknown')),
                                "username": getattr(entity, 'username', None),
                                "entity": entity,
                                "is_public": getattr(entity, 'username', None) is not None
                            })
                    except Exception as e:
                        logger.error(f"Не удалось получить entity из фильтра: {e}")

            return {
                "success": True,
                "title": str(getattr(target_filter, 'title', 'Folder')) if target_filter else 'Folder',
                "chats": chats_data,
                "filter_id": filter_id,
                "just_imported": False
            }

        elif isinstance(check, tl_types.chatlists.ChatlistInvite):
            # Импортируем
            peers = []
            chats_info = {}

            for chat in getattr(check, 'chats', []):
                try:
                    raw_id = getattr(chat, 'id', None)
                    if hasattr(raw_id, 'text'):
                        raw_id = int(raw_id.text)
                    chat_id = int(raw_id) if raw_id else None

                    if not chat_id:
                        continue

                    access_hash = getattr(chat, 'access_hash', 0) or 0
                    is_channel = getattr(chat, 'broadcast', False) or getattr(chat, 'megagroup', False)

                    if is_channel and access_hash:
                        peer = InputPeerChannel(chat_id, access_hash)
                    else:
                        peer = InputPeerChat(chat_id)

                    peers.append(peer)
                    chats_info[chat_id] = {
                        "id": chat_id,
                        "title": str(getattr(chat, 'title', 'Unknown')),
                        "access_hash": access_hash
                    }
                except Exception as e:
                    logger.error(f"Ошибка формирования peer: {e}")

            if not peers:
                return {"success": False, "error": "no_peers"}

            try:
                updates = await client(functions.chatlists.JoinChatlistInviteRequest(
                    slug=slug,
                    peers=peers
                ))

                filter_id = None
                for update in updates.updates:
                    if isinstance(update, tl_types.UpdateDialogFilter):
                        filter_id = update.id
                        break

                # Ждем синхронизации
                await asyncio.sleep(5)
                await client.get_dialogs(limit=300)
                await asyncio.sleep(2)

                # Получаем entity
                chats_data = []
                for chat_id, info in chats_info.items():
                    try:
                        entity = await client.get_entity(chat_id)
                        if entity:
                            chats_data.append({
                                "id": entity.id,
                                "title": str(getattr(entity, 'title', info['title'])),
                                "username": getattr(entity, 'username', None),
                                "entity": entity,
                                "is_public": getattr(entity, 'username', None) is not None
                            })
                    except Exception as e:
                        logger.error(f"Не удалось получить entity для {chat_id}: {e}")

                title = getattr(check, 'title', 'Folder')
                if hasattr(title, 'text'):
                    title = str(title.text)
                else:
                    title = str(title)

                return {
                    "success": True,
                    "title": title,
                    "chats": chats_data,
                    "filter_id": filter_id,
                    "just_imported": True
                }

            except errors.ChannelsTooMuchError:
                return {"success": False, "error": "too_many_channels"}
            except Exception as e:
                error_str = str(e).lower()
                if "too many" in error_str or "limit" in error_str:
                    return {"success": False, "error": "too_many_channels"}
                logger.error(f"Ошибка импорта: {e}")
                return {"success": False, "error": f"Import failed: {str(e)}"}

        return {"success": False, "error": "unknown_response"}

    except errors.InviteHashExpiredError:
        return {"success": False, "error": "expired"}
    except errors.InviteHashInvalidError:
        return {"success": False, "error": "invalid"}
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return {"success": False, "error": str(e)}


async def cleanup_folder_and_leave_chats(client: TelegramClient, filter_id: Optional[int], chats: List[Dict],
                                         just_imported: bool):
    try:
        # Удаляем папку (UpdateDialogFilter с filter=None)
        if filter_id:
            try:
                await client(functions.messages.UpdateDialogFilterRequest(
                    id=filter_id,
                    filter=None
                ))
                logger.info(f"Папка {filter_id} удалена")
            except Exception as e:
                logger.error(f"Ошибка удаления папки {filter_id}: {e}")

        # Выходим из чатов только если мы только что вступили
        if just_imported and chats:
            logger.info(f"Выход из {len(chats)} чатов...")
            for chat in chats:
                try:
                    entity = chat.get("entity")
                    if not entity:
                        try:
                            entity = await client.get_entity(chat["id"])
                        except:
                            continue

                    # Выход из канала/супергруппы
                    if isinstance(entity, Channel):
                        try:
                            await client(functions.channels.LeaveChannelRequest(channel=entity))
                            logger.info(f"Вышли из канала {chat.get('id')}")
                            await asyncio.sleep(0.5)
                        except errors.ChannelPrivateError:
                            pass

                    # Выход из группы
                    elif isinstance(entity, Chat):
                        try:
                            await client(functions.messages.DeleteChatUserRequest(
                                chat_id=entity.id,
                                user_id=InputPeerSelf()
                            ))
                            logger.info(f"Вышли из чата {chat.get('id')}")
                            await asyncio.sleep(0.5)
                        except errors.UserNotParticipantError:
                            pass

                except Exception as e:
                    logger.error(f"Ошибка выхода из чата {chat.get('id')}: {e}")

    except Exception as e:
        logger.error(f"Ошибка очистки: {e}")