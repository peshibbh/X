// Lenwy Was Here: Lenwy Whatsmeow Engine
// Disclaimer: This code is provided as-is and may not be suitable for production use. Use at your own risk.
// Under the MIT License (MIT). Copyright (c) 2024 Lenwy. All rights reserved.

// Thanks to the following libraries:
// - github.com/mattn/go-sqlite3
// - go.mau.fi/whatsmeow

package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

type IPCEvent struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

type IPCCommand struct {
	Action  string          `json:"action"`
	ID      string          `json:"id"`
	Payload json.RawMessage `json:"payload"`
}

type RowItem struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	ID          string `json:"id"`
	Header      string `json:"header"`
}

type SectionItem struct {
	Title string    `json:"title"`
	Rows  []RowItem `json:"rows"`
}

type ButtonItem struct {
	Text     string        `json:"text"`
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	URL      string        `json:"url"`
	Sections []SectionItem `json:"sections"`
}

type SendMessagePayload struct {
	JID          string       `json:"jid"`
	Text         string       `json:"text"`
	Footer       string       `json:"footer"`
	Buttons      []ButtonItem `json:"buttons"`
	QuotedID     string       `json:"quotedId"`
	QuotedSender string       `json:"quotedSender"`
}

type SendMediaPayload struct {
	JID          string       `json:"jid"`
	MediaType    string       `json:"mediaType"`
	FilePath     string       `json:"filePath"`
	Caption      string       `json:"caption"`
	FileName     string       `json:"fileName"`
	Buttons      []ButtonItem `json:"buttons"`
	QuotedID     string       `json:"quotedId"`
	QuotedSender string       `json:"quotedSender"`
}

type DownloadMediaPayload struct {
	MessageID string `json:"messageId"`
	OutputDir string `json:"outputDir"`
}

type UploadMediaPayload struct {
	FilePath  string `json:"filePath"`
	MediaType string `json:"mediaType"`
}

type DownloadMediaDirectPayload struct {
	URL        string `json:"url"`
	DirectPath string `json:"directPath"`
	MediaKey   string `json:"mediaKey"`
	MediaType  string `json:"mediaType"`
}

type PairPhonePayload struct {
	Phone string `json:"phone"`
}

type GroupJIDPayload struct {
	JID string `json:"jid"`
}

type GroupParticipantsPayload struct {
	JID          string   `json:"jid"`
	Participants []string `json:"participants"`
	Action       string   `json:"action"`
}

type GroupSettingPayload struct {
	JID     string `json:"jid"`
	Setting string `json:"setting"`
}

var (
	msgCache      = make(map[string]*events.Message)
	msgCacheMutex sync.RWMutex
)

func sendIPC(event string, data interface{}) {
	payload, err := json.Marshal(IPCEvent{
		Event: event,
		Data:  data,
	})

	if err == nil {
		fmt.Println(string(payload))
	}
}

func extractMediaMessage(msg *waProto.Message) (*waProto.Message, string, string) {
	if msg == nil {
		return nil, "", ""
	}

	if img := msg.GetImageMessage(); img != nil {
		ext := ".jpg"

		if img.GetMimetype() == "image/png" {
			ext = ".png"
		}

		return msg, ext, "image"
	}

	if msg.GetVideoMessage() != nil {
		return msg, ".mp4", "video"
	}

	if msg.GetStickerMessage() != nil {
		return msg, ".webp", "sticker"
	}

	if aud := msg.GetAudioMessage(); aud != nil {
		ext := ".mp3"

		if strings.Contains(aud.GetMimetype(), "ogg") {
			ext = ".ogg"
		}

		return msg, ext, "sound"
	}

	if doc := msg.GetDocumentMessage(); doc != nil {
		ext := filepath.Ext(doc.GetFileName())

		if ext == "" {
			ext = ".bin"
		}

		return msg, ext, "document"
	}

	if extMsg := msg.GetExtendedTextMessage(); extMsg != nil &&
		extMsg.ContextInfo != nil &&
		extMsg.ContextInfo.QuotedMessage != nil {
		return extractMediaMessage(extMsg.ContextInfo.QuotedMessage)
	}

	return nil, "", ""
}

func parseMessageContent(msg *waProto.Message) (string, string) {
	if msg == nil {
		return "Chat", ""
	}

	if text := msg.GetConversation(); text != "" {
		return "Chat", text
	}

	if extText := msg.GetExtendedTextMessage(); extText != nil {
		return "Chat", extText.GetText()
	}

	if img := msg.GetImageMessage(); img != nil {
		if caption := img.GetCaption(); caption != "" {
			return "Image", caption
		}

		return "Image", "Mengirimkan Gambar"
	}

	if vid := msg.GetVideoMessage(); vid != nil {
		if caption := vid.GetCaption(); caption != "" {
			return "Video", caption
		}

		return "Video", "Mengirimkan Video"
	}

	if msg.GetStickerMessage() != nil {
		return "Sticker", "Mengirimkan Stiker"
	}

	if doc := msg.GetDocumentMessage(); doc != nil {
		if fileName := doc.GetFileName(); fileName != "" {
			return "Document", fileName
		}

		return "Document", "Mengirimkan Dokumen"
	}

	if msg.GetAudioMessage() != nil {
		return "Audio", "Mengirimkan Audio"
	}

	if interactive := msg.GetInteractiveResponseMessage(); interactive != nil {
		if nf := interactive.GetNativeFlowResponseMessage(); nf != nil {
			paramsJson := nf.GetParamsJson()
			if paramsJson != "" {
				var params map[string]interface{}
				if err := json.Unmarshal([]byte(paramsJson), &params); err == nil {
					if id, ok := params["id"].(string); ok && id != "" {
						return "interactiveResponseMessage", id
					}
					if title, ok := params["title"].(string); ok && title != "" {
						return "interactiveResponseMessage", title
					}
				}
				return "interactiveResponseMessage", paramsJson
			}
		}
		return "interactiveResponseMessage", ""
	}

	if btnResp := msg.GetButtonsResponseMessage(); btnResp != nil {
		return "buttonsResponseMessage", btnResp.GetSelectedButtonId()
	}

	if listResp := msg.GetListResponseMessage(); listResp != nil {
		if single := listResp.GetSingleSelectReply(); single != nil {
			return "listResponseMessage", single.GetSelectedRowId()
		}
	}

	return "Chat", ""
}

func buildMediaMessageForDownload(p DownloadMediaDirectPayload, mediaKey []byte) *waProto.Message {
	switch p.MediaType {

	case "image", "thumbnail-link", "thumb", "product":
		return &waProto.Message{
			ImageMessage: &waProto.ImageMessage{
				URL:        &p.URL,
				DirectPath: &p.DirectPath,
				MediaKey:   mediaKey,
			},
		}

	case "video":
		return &waProto.Message{
			VideoMessage: &waProto.VideoMessage{
				URL:        &p.URL,
				DirectPath: &p.DirectPath,
				MediaKey:   mediaKey,
			},
		}

	case "audio", "sound", "ptt":
		return &waProto.Message{
			AudioMessage: &waProto.AudioMessage{
				URL:        &p.URL,
				DirectPath: &p.DirectPath,
				MediaKey:   mediaKey,
			},
		}

	case "sticker":
		return &waProto.Message{
			StickerMessage: &waProto.StickerMessage{
				URL:        &p.URL,
				DirectPath: &p.DirectPath,
				MediaKey:   mediaKey,
			},
		}

	case "document":
		return &waProto.Message{
			DocumentMessage: &waProto.DocumentMessage{
				URL:        &p.URL,
				DirectPath: &p.DirectPath,
				MediaKey:   mediaKey,
			},
		}
	}

	return nil
}

func buildInteractiveButtons(title, footer string, buttons []ButtonItem) *waProto.Message {
	if len(buttons) == 0 {
		return nil
	}

	var protoButtons []*waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton
	for _, b := range buttons {
		bText := b.Text
		bID := b.ID
		if bID == "" {
			bID = bText
		}

		var paramsJson []byte
		var btnName string

		if len(b.Sections) > 0 || b.Type == "single_select" || b.Type == "list" {
			btnName = "single_select"
			var secs []map[string]interface{}
			for _, sec := range b.Sections {
				var rows []map[string]interface{}
				for _, r := range sec.Rows {
					rowMap := map[string]interface{}{
						"id":          r.ID,
						"title":       r.Title,
						"description": r.Description,
					}
					if r.Header != "" {
						rowMap["header"] = r.Header
					}
					rows = append(rows, rowMap)
				}
				secs = append(secs, map[string]interface{}{
					"title": sec.Title,
					"rows":  rows,
				})
			}
			paramsJson, _ = json.Marshal(map[string]interface{}{
				"title":    bText,
				"sections": secs,
			})
		} else if b.Type == "url" && b.URL != "" {
			btnName = "cta_url"
			paramsJson, _ = json.Marshal(map[string]string{
				"display_text": bText,
				"url":          b.URL,
			})
		} else {
			btnName = "quick_reply"
			paramsJson, _ = json.Marshal(map[string]string{
				"display_text": bText,
				"id":           bID,
			})
		}

		protoButtons = append(protoButtons, &waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
			Name:             proto.String(btnName),
			ButtonParamsJson: proto.String(string(paramsJson)),
		})
	}

	interactiveMsg := &waProto.InteractiveMessage{
		Body: &waProto.InteractiveMessage_Body{
			Text: proto.String(title),
		},
		InteractiveMessage: &waProto.InteractiveMessage_NativeFlowMessage_{
			NativeFlowMessage: &waProto.InteractiveMessage_NativeFlowMessage{
				Buttons: protoButtons,
			},
		},
	}

	if footer != "" {
		interactiveMsg.Footer = &waProto.InteractiveMessage_Footer{
			Text: proto.String(footer),
		}
	}

	return &waProto.Message{
		ViewOnceMessage: &waProto.FutureProofMessage{
			Message: &waProto.Message{
				InteractiveMessage: interactiveMsg,
			},
		},
	}
}

// Lenwy Disclaimer: This code is provided as-is and may not be suitable for production use. Use at your own risk.
func main() {
	ctx := context.Background()

	sessionName := "lenwy"

	if len(os.Args) > 1 && os.Args[1] != "" {
		sessionName = os.Args[1]
	}

	sessionDir := filepath.Join("../sessions", sessionName)

	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		panic(err)
	}

	dbPath := filepath.Join(sessionDir, "whatsmeow.db")

	dbLog := waLog.Stdout("Database", "ERROR", true)

	container, err := sqlstore.New(
		ctx,
		"sqlite3",
		fmt.Sprintf("file:%s?_foreign_keys=on", dbPath),
		dbLog,
	)

	if err != nil {
		panic(err)
	}

	deviceStore, err := container.GetFirstDevice(ctx)

	if err != nil {
		panic(err)
	}

	clientLog := waLog.Stdout("Client", "ERROR", true)

	client := whatsmeow.NewClient(deviceStore, clientLog)

	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {

		case *events.Connected:
			botJid := ""

			if client.Store.ID != nil {
				botJid = client.Store.ID.ToNonAD().String()
			}

			sendIPC("connection.update", map[string]string{
				"status": "open",
				"botJid": botJid,
			})

		case *events.Disconnected:
			sendIPC("connection.update", map[string]interface{}{
				"status": "connecting",
				"reason": "connection_lost",
			})

		case *events.LoggedOut:
			sendIPC("connection.update", map[string]interface{}{
				"status": "close",
				"reason": "logged_out",
			})

			container.Close()
			os.Exit(0)

		case *events.Message:
			msgCacheMutex.Lock()
			msgCache[v.Info.ID] = v
			msgCacheMutex.Unlock()

			msgType, body := parseMessageContent(v.Message)

			var quotedID string
			var quotedSender string

			if ext := v.Message.GetExtendedTextMessage(); ext != nil &&
				ext.ContextInfo != nil {

				quotedID = ext.ContextInfo.GetStanzaID()
				quotedSender = ext.ContextInfo.GetParticipant()
			}

			botJid := ""

			if client.Store.ID != nil {
				botJid = client.Store.ID.ToNonAD().String()
			}

			// Prioritaskan PNJID (Phone Number JID)
			senderJID := v.Info.Sender.ToNonAD()

			sendIPC("messages.upsert", map[string]interface{}{
				"id":           v.Info.ID,
				"chat":         v.Info.Chat.String(),
				"sender":       senderJID.User,
				"senderJid":    senderJID.String(),
				"pushName":     v.Info.PushName,
				"isFromMe":     v.Info.IsFromMe,
				"timestamp":    v.Info.Timestamp.Unix(),
				"type":         msgType,
				"body":         body,
				"quotedId":     quotedID,
				"quotedSender": quotedSender,
				"botJid":       botJid,
			})
		}
	})

	err = client.Connect()

	if err != nil {
		sendIPC("error", map[string]string{
			"message": "Gagal connect: " + err.Error(),
		})

		container.Close()
		return
	}

	go func() {
		scanner := bufio.NewScanner(os.Stdin)

		for scanner.Scan() {
			line := scanner.Text()

			var cmd IPCCommand

			if err := json.Unmarshal([]byte(line), &cmd); err != nil {
				continue
			}

			switch cmd.Action {

			// Lenwy Was Here: Shutdown Engine
			case "shutdown":
				if client.IsConnected() {
					client.Disconnect()
				}

				container.Close()

				sendIPC("response", map[string]interface{}{
					"id":     cmd.ID,
					"status": "ok",
				})

				os.Exit(0)

			// Pairing Kode
			case "requestPairingCode":
				var p PairPhonePayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					if client.Store.ID != nil {
						sendIPC("response", map[string]interface{}{
							"id":    cmd.ID,
							"error": "Client sudah terhubung/login (session aktif)",
						})
						continue
					}

					if !client.IsConnected() {
						sendIPC("response", map[string]interface{}{
							"id":    cmd.ID,
							"error": "Client belum terhubung ke server WA",
						})
						continue
					}

					code, err := client.PairPhone(
						ctx,
						p.Phone,
						true,
						whatsmeow.PairClientChrome,
						"Chrome (Linux)",
					)

					if err == nil {
						sendIPC("pairing_code", map[string]string{
							"code": code,
						})
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "ok",
							"code":   code,
						})
					} else {
						sendIPC("response", map[string]interface{}{
							"id":    cmd.ID,
							"error": "Gagal pair: " + err.Error(),
						})
					}
				} else {
					sendIPC("response", map[string]interface{}{
						"id":    cmd.ID,
						"error": "Invalid payload",
					})
				}

			// Group Metadata
			case "getGroupMetadata":
				var p GroupJIDPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						info, err := client.GetGroupInfo(ctx, targetJID)

						if err == nil {
							var participants []map[string]interface{}
							isBotAdmin := false

							botUser := ""
							botLidUser := ""

							if client.Store.ID != nil {
								botUser = client.Store.ID.User
							}

							if client.Store.LID.User != "" {
								botLidUser = client.Store.LID.User
							}

							for _, m := range info.Participants {
								adminType := ""

								if m.IsSuperAdmin {
									adminType = "superadmin"
								} else if m.IsAdmin {
									adminType = "admin"
								}

								pUser := m.JID.User

								isBot :=
									(botUser != "" && pUser == botUser) ||
										(botLidUser != "" && pUser == botLidUser)

								if isBot &&
									(m.IsAdmin || m.IsSuperAdmin) {
									isBotAdmin = true
								}

								participants = append(
									participants,
									map[string]interface{}{
										"id":    m.JID.String(),
										"user":  m.JID.User,
										"admin": adminType,
										"isBot": isBot,
									},
								)
							}

							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
								"resp": map[string]interface{}{
									"id":           info.JID.String(),
									"subject":      info.Name,
									"owner":        info.OwnerJID.String(),
									"isBotAdmin":   isBotAdmin,
									"participants": participants,
								},
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Invite Link
			case "getGroupInviteLink":
				var p GroupJIDPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						link, err := client.GetGroupInviteLink(
							ctx,
							targetJID,
							false,
						)

						if err == nil {
							if !strings.HasPrefix(link, "https://") {
								link =
									"https://chat.whatsapp.com/" +
										link
							}

							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
								"resp":   link,
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Group Participants
			case "updateGroupParticipants":
				var p GroupParticipantsPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Invalid Group JID",
						})
						continue
					}

					var participantJIDs []types.JID

					for _, user := range p.Participants {
						uJID, err := types.ParseJID(user)

						if err == nil {
							participantJIDs =
								append(participantJIDs, uJID)
						}
					}

					var waAction whatsmeow.ParticipantChange

					switch p.Action {

					case "add":
						waAction = whatsmeow.ParticipantChangeAdd

					case "remove":
						waAction = whatsmeow.ParticipantChangeRemove

					case "promote":
						waAction =
							whatsmeow.ParticipantChangePromote

					case "demote":
						waAction =
							whatsmeow.ParticipantChangeDemote

					default:
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Aksi tidak dikenal",
						})
						continue
					}

					res, err := client.UpdateGroupParticipants(
						ctx,
						targetJID,
						participantJIDs,
						waAction,
					)

					if err == nil {
						inviteRequired := false
						var inviteLink string

						if p.Action == "add" {
							for _, item := range res {
								if item.Error == 403 {
									inviteRequired = true
									break
								}
							}

							if inviteRequired {
								link, errLink :=
									client.GetGroupInviteLink(
										ctx,
										targetJID,
										false,
									)

								if errLink == nil {
									if !strings.HasPrefix(
										link,
										"https://",
									) {
										link =
											"https://chat.whatsapp.com/" +
												link
									}

									inviteLink = link
								}
							}
						}

						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "ok",
							"resp": map[string]interface{}{
								"results":        res,
								"inviteRequired": inviteRequired,
								"inviteLink":     inviteLink,
							},
						})
					} else {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  err.Error(),
						})
					}
				}

			// Group Settings
			case "updateGroupSettings":
				var p GroupSettingPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Invalid Group JID",
						})
						continue
					}

					switch p.Setting {

					case "announcement":
						err = client.SetGroupAnnounce(
							ctx,
							targetJID,
							true,
						)

					case "not_announcement":
						err = client.SetGroupAnnounce(
							ctx,
							targetJID,
							false,
						)

					case "locked":
						err = client.SetGroupLocked(
							ctx,
							targetJID,
							true,
						)

					case "unlocked":
						err = client.SetGroupLocked(
							ctx,
							targetJID,
							false,
						)

					default:
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Setelan tidak dikenal",
						})
						continue
					}

					if err == nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "ok",
							"resp":   "success",
						})
					} else {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  err.Error(),
						})
					}
				}

			// Group Subject
			case "setGroupSubject":
				var p struct {
					JID     string `json:"jid"`
					Subject string `json:"subject"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						err = client.SetGroupName(
							ctx,
							targetJID,
							p.Subject,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Group Description
			case "setGroupDescription":
				var p struct {
					JID         string `json:"jid"`
					Description string `json:"description"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						err = client.SetGroupTopic(
							ctx,
							targetJID,
							"",
							"",
							p.Description,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Revoke Group Invite Link
			case "revokeGroupInviteLink":
				var p GroupJIDPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						link, err := client.GetGroupInviteLink(
							ctx,
							targetJID,
							true,
						)

						if err == nil {
							if !strings.HasPrefix(link, "https://") {
								link =
									"https://chat.whatsapp.com/" +
										link
							}

							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
								"resp":   link,
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Download Media
			case "downloadMedia":
				var p DownloadMediaPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					msgCacheMutex.RLock()
					evtMsg, exists := msgCache[p.MessageID]
					msgCacheMutex.RUnlock()

					if !exists {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Pesan tidak ditemukan di cache",
						})
						continue
					}

					mediaMsg, ext, defaultName :=
						extractMediaMessage(evtMsg.Message)

					if mediaMsg == nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Pesan tidak mengandung media",
						})
						continue
					}

					data, err := client.DownloadAny(
						ctx,
						mediaMsg,
					)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal download media: " + err.Error(),
						})
						continue
					}

					outputDir := p.OutputDir

					if outputDir == "" {
						outputDir = "."
					}

					_ = os.MkdirAll(outputDir, 0755)

					fileName := fmt.Sprintf(
						"%s%s",
						defaultName,
						ext,
					)

					finalPath :=
						filepath.Join(outputDir, fileName)

					counter := 2

					for {
						if _, err := os.Stat(finalPath); os.IsNotExist(err) {
							break
						}

						fileName = fmt.Sprintf(
							"%s%d%s",
							defaultName,
							counter,
							ext,
						)

						finalPath =
							filepath.Join(outputDir, fileName)

						counter++
					}

					err = os.WriteFile(
						finalPath,
						data,
						0644,
					)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal simpan file: " + err.Error(),
						})
						continue
					}

					sendIPC("response", map[string]interface{}{
						"id":     cmd.ID,
						"status": "ok",
						"resp": map[string]interface{}{
							"filePath":  finalPath,
							"fileName":  fileName,
							"mediaType": defaultName,
							"ext":       ext,
						},
					})
				}

			// Upload Media (Media engine, no send)
			case "uploadMedia", "media:upload":
				var p UploadMediaPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					if !client.IsConnected() {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Go engine belum terhubung ke server WA",
						})
						continue
					}

					if p.FilePath == "" {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "filePath kosong",
						})
						continue
					}

					fileData, err := os.ReadFile(p.FilePath)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "File tidak ditemukan: " + err.Error(),
						})
						continue
					}

					waMediaType := whatsmeow.MediaDocument

					switch p.MediaType {

					case "image", "sticker", "thumbnail-link", "thumb":
						waMediaType = whatsmeow.MediaImage

					case "video":
						waMediaType = whatsmeow.MediaVideo

					case "audio", "sound", "ptt":
						waMediaType = whatsmeow.MediaAudio
					}

					uploadResp, err := client.Upload(
						ctx,
						fileData,
						waMediaType,
					)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal upload: " + err.Error(),
						})
						continue
					}

					sendIPC("response", map[string]interface{}{
						"id":     cmd.ID,
						"status": "ok",
						"resp": map[string]interface{}{
							"url":           uploadResp.URL,
							"directPath":    uploadResp.DirectPath,
							"mediaKey":      base64.StdEncoding.EncodeToString(uploadResp.MediaKey),
							"fileEncSha256": base64.StdEncoding.EncodeToString(uploadResp.FileEncSHA256),
							"fileSha256":    base64.StdEncoding.EncodeToString(uploadResp.FileSHA256),
						},
					})
				}

			// Download Media Direct (Media engine, from directPath/mediaKey)
			case "downloadMediaDirect", "media:download":
				var p DownloadMediaDirectPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					mediaKey, err := base64.StdEncoding.DecodeString(p.MediaKey)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "MediaKey tidak valid",
						})
						continue
					}

					mediaMsg := buildMediaMessageForDownload(p, mediaKey)

					if mediaMsg == nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "MediaType tidak dikenal: " + p.MediaType,
						})
						continue
					}

					data, err := client.DownloadAny(ctx, mediaMsg)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal download media: " + err.Error(),
						})
						continue
					}

					fileName := fmt.Sprintf(
						"dl_%s_%d",
						p.MediaType,
						time.Now().UnixNano(),
					)

					finalPath := filepath.Join(os.TempDir(), fileName)

					err = os.WriteFile(finalPath, data, 0644)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal simpan file: " + err.Error(),
						})
						continue
					}

					sendIPC("response", map[string]interface{}{
						"id":     cmd.ID,
						"status": "ok",
						"resp": map[string]interface{}{
							"filePath":   finalPath,
							"fileLength": len(data),
						},
					})
				}

			// Send Message
			case "sendMessage", "send_message":
				var p SendMessagePayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						var msg *waProto.Message

						if len(p.Buttons) > 0 {
							msg = buildInteractiveButtons(p.Text, p.Footer, p.Buttons)
						} else if p.QuotedID != "" {
							contextInfo := &waProto.ContextInfo{
								StanzaID:    &p.QuotedID,
								Participant: &p.QuotedSender,
							}

							msg = &waProto.Message{
								ExtendedTextMessage: &waProto.ExtendedTextMessage{
									Text:        &p.Text,
									ContextInfo: contextInfo,
								},
							}
						} else {
							msg = &waProto.Message{
								Conversation: &p.Text,
							}
						}

						resp, err := client.SendMessage(
							ctx,
							targetJID,
							msg,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
								"resp":   resp,
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// React Message
			case "reactMessage":
				var p struct {
					JID string `json:"jid"`
					Key struct {
						ID          string `json:"id"`
						Participant string `json:"participant"`
					} `json:"key"`
					Text string `json:"text"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						var senderJID types.JID

						if p.Key.Participant != "" {
							senderJID, _ =
								types.ParseJID(
									p.Key.Participant,
								)
						}

						reactMsg := client.BuildReaction(
							targetJID,
							senderJID,
							p.Key.ID,
							p.Text,
						)

						_, err = client.SendMessage(
							ctx,
							targetJID,
							reactMsg,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Delete Message
			case "deleteMessage":
				var p struct {
					JID string `json:"jid"`
					Key struct {
						ID          string `json:"id"`
						Participant string `json:"participant"`
					} `json:"key"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						var senderJID types.JID

						if p.Key.Participant != "" {
							senderJID, _ =
								types.ParseJID(
									p.Key.Participant,
								)
						}

						revokeMsg := client.BuildRevoke(
							targetJID,
							senderJID,
							p.Key.ID,
						)

						_, err = client.SendMessage(
							ctx,
							targetJID,
							revokeMsg,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Edit Message
			case "editMessage":
				var p struct {
					JID string `json:"jid"`
					Key struct {
						ID string `json:"id"`
					} `json:"key"`
					Text string `json:"text"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						newContent := &waE2E.Message{
							Conversation: proto.String(p.Text),
						}

						editMsg := client.BuildEdit(
							targetJID,
							p.Key.ID,
							newContent,
						)

						_, err = client.SendMessage(
							ctx,
							targetJID,
							editMsg,
						)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Send Media
			case "sendMedia", "send_media":
				var p SendMediaPayload

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Invalid JID",
						})
						continue
					}

					fileData, err := os.ReadFile(p.FilePath)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "File tidak ditemukan: " + err.Error(),
						})
						continue
					}

					var waMediaType whatsmeow.MediaType

					switch p.MediaType {

					case "image", "sticker":
						waMediaType = whatsmeow.MediaImage

					case "video":
						waMediaType = whatsmeow.MediaVideo

					case "audio", "sound", "ptt":
						waMediaType = whatsmeow.MediaAudio

					default:
						waMediaType = whatsmeow.MediaDocument
					}

					uploadResp, err := client.Upload(
						ctx,
						fileData,
						waMediaType,
					)

					if err != nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  "Gagal Upload: " + err.Error(),
						})
						continue
					}

					mimeType :=
						http.DetectContentType(fileData)

					fileLen := uint64(len(fileData))

					var contextInfo *waProto.ContextInfo

					if p.QuotedID != "" {
						contextInfo = &waProto.ContextInfo{
							StanzaID:    &p.QuotedID,
							Participant: &p.QuotedSender,
						}
					}

					var msg *waProto.Message

					switch p.MediaType {

					case "image":
						msg = &waProto.Message{
							ImageMessage:
								&waProto.ImageMessage{
									URL:           &uploadResp.URL,
									DirectPath:    &uploadResp.DirectPath,
									MediaKey:      uploadResp.MediaKey,
									FileSHA256:    uploadResp.FileSHA256,
									FileEncSHA256: uploadResp.FileEncSHA256,
									FileLength:    &fileLen,
									Mimetype:      &mimeType,
									Caption:       &p.Caption,
									ContextInfo:   contextInfo,
								},
						}

					// Lenwy Was Here: Sticker Support
					case "sticker":
						mimeSticker := "image/webp"

						msg = &waProto.Message{
							StickerMessage:
								&waProto.StickerMessage{
									URL:           &uploadResp.URL,
									DirectPath:    &uploadResp.DirectPath,
									MediaKey:      uploadResp.MediaKey,
									FileSHA256:    uploadResp.FileSHA256,
									FileEncSHA256: uploadResp.FileEncSHA256,
									FileLength:    &fileLen,
									Mimetype:      &mimeSticker,
									ContextInfo:   contextInfo,
								},
						}

					case "video":
						mimeVideo := "video/mp4"

						msg = &waProto.Message{
							VideoMessage:
								&waProto.VideoMessage{
									URL:           &uploadResp.URL,
									DirectPath:    &uploadResp.DirectPath,
									MediaKey:      uploadResp.MediaKey,
									FileSHA256:    uploadResp.FileSHA256,
									FileEncSHA256: uploadResp.FileEncSHA256,
									FileLength:    &fileLen,
									Mimetype:      &mimeVideo,
									Caption:       &p.Caption,
									ContextInfo:   contextInfo,
								},
						}

					case "audio", "sound", "ptt":
						isPTT := p.MediaType == "ptt"
						mimeAudio := mimeType

						if isPTT {
							mimeAudio =
								"audio/ogg; codecs=opus"
						}

						msg = &waProto.Message{
							AudioMessage:
								&waProto.AudioMessage{
									URL:           &uploadResp.URL,
									DirectPath:    &uploadResp.DirectPath,
									MediaKey:      uploadResp.MediaKey,
									FileSHA256:    uploadResp.FileSHA256,
									FileEncSHA256: uploadResp.FileEncSHA256,
									FileLength:    &fileLen,
									Mimetype:      &mimeAudio,
									PTT:           &isPTT,
									ContextInfo:   contextInfo,
								},
						}

					default:
						docName := p.FileName

						if docName == "" {
							docName =
								filepath.Base(p.FilePath)
						}

						msg = &waProto.Message{
							DocumentMessage:
								&waProto.DocumentMessage{
									URL:           &uploadResp.URL,
									DirectPath:    &uploadResp.DirectPath,
									MediaKey:      uploadResp.MediaKey,
									FileSHA256:    uploadResp.FileSHA256,
									FileEncSHA256: uploadResp.FileEncSHA256,
									FileLength:    &fileLen,
									Mimetype:      &mimeType,
									Title:         &docName,
									FileName:      &docName,
									ContextInfo:   contextInfo,
								},
						}
					}

					if len(p.Buttons) > 0 && p.MediaType == "image" && msg.ImageMessage != nil {
						var protoButtons []*waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton
						for _, b := range p.Buttons {
							bText := b.Text
							bID := b.ID
							if bID == "" {
								bID = bText
							}
							var paramsJson []byte
							var btnName string

							if len(b.Sections) > 0 || b.Type == "single_select" || b.Type == "list" {
								btnName = "single_select"
								var secs []map[string]interface{}
								for _, sec := range b.Sections {
									var rows []map[string]interface{}
									for _, r := range sec.Rows {
										rowMap := map[string]interface{}{
											"id":          r.ID,
											"title":       r.Title,
											"description": r.Description,
										}
										if r.Header != "" {
											rowMap["header"] = r.Header
										}
										rows = append(rows, rowMap)
									}
									secs = append(secs, map[string]interface{}{
										"title": sec.Title,
										"rows":  rows,
									})
								}
								paramsJson, _ = json.Marshal(map[string]interface{}{
									"title":    bText,
									"sections": secs,
								})
							} else if b.Type == "url" && b.URL != "" {
								btnName = "cta_url"
								paramsJson, _ = json.Marshal(map[string]string{
									"display_text": bText,
									"url":          b.URL,
								})
							} else {
								btnName = "quick_reply"
								paramsJson, _ = json.Marshal(map[string]string{
									"display_text": bText,
									"id":           bID,
								})
							}

							protoButtons = append(protoButtons, &waProto.InteractiveMessage_NativeFlowMessage_NativeFlowButton{
								Name:             proto.String(btnName),
								ButtonParamsJson: proto.String(string(paramsJson)),
							})
						}

						interactiveMsg := &waProto.InteractiveMessage{
							Header: &waProto.InteractiveMessage_Header{
								Title:              proto.String(""),
								HasMediaAttachment: proto.Bool(true),
								Media: &waProto.InteractiveMessage_Header_ImageMessage{
									ImageMessage: msg.ImageMessage,
								},
							},
							Body: &waProto.InteractiveMessage_Body{
								Text: proto.String(p.Caption),
							},
							InteractiveMessage: &waProto.InteractiveMessage_NativeFlowMessage_{
								NativeFlowMessage: &waProto.InteractiveMessage_NativeFlowMessage{
									Buttons: protoButtons,
								},
							},
						}

						msg = &waProto.Message{
							ViewOnceMessage: &waProto.FutureProofMessage{
								Message: &waProto.Message{
									InteractiveMessage: interactiveMsg,
								},
							},
						}
					}

					resp, err := client.SendMessage(
						ctx,
						targetJID,
						msg,
					)

					if err == nil {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "ok",
							"resp":   resp,
						})
					} else {
						sendIPC("response", map[string]interface{}{
							"id":     cmd.ID,
							"status": "error",
							"error":  err.Error(),
						})
					}
				}
			// Block / Unblock User
			case "updateBlockStatus", "blockUser":
				var p struct {
					JID    string `json:"jid"`
					Action string `json:"action"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						var action events.BlocklistAction
						if p.Action == "block" {
							action = events.BlocklistActionBlock
						} else {
							action = events.BlocklistActionUnblock
						}

						_, err = client.UpdateBlocklist(targetJID, action)

						if err == nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  err.Error(),
							})
						}
					}
				}

			// Profile Picture URL
			case "profilePictureUrl", "getProfilePicture":
				var p struct {
					JID string `json:"jid"`
				}

				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)

					if err == nil {
						info, err := client.GetProfilePictureInfo(
							ctx,
							targetJID,
							&whatsmeow.GetProfilePictureParams{},
						)

						if err == nil && info != nil {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "ok",
								"resp":   info.URL,
							})
						} else {
							sendIPC("response", map[string]interface{}{
								"id":     cmd.ID,
								"status": "error",
								"error":  "Profile picture tidak ditemukan",
							})
						}
					}
				}

			// Leave Group
			case "groupLeave", "leaveGroup":
				var p GroupJIDPayload
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					targetJID, err := types.ParseJID(p.JID)
					if err == nil {
						err = client.LeaveGroup(ctx, targetJID)
						if err == nil {
							sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok"})
						} else {
							sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "error", "error": err.Error()})
						}
					}
				}

			// Create Group
			case "groupCreate", "createGroup":
				var p struct {
					Subject      string   `json:"subject"`
					Participants []string `json:"participants"`
				}
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					var pJIDs []types.JID
					for _, u := range p.Participants {
						if uJID, err := types.ParseJID(u); err == nil {
							pJIDs = append(pJIDs, uJID)
						}
					}
					res, err := client.CreateGroup(ctx, whatsmeow.ReqCreateGroup{Name: p.Subject, Participants: pJIDs})
					if err == nil {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok", "resp": map[string]interface{}{"id": res.JID.String(), "subject": res.Name}})
					} else {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "error", "error": err.Error()})
					}
				}

			// Join / Accept Group Invite
			case "groupAcceptInvite", "joinGroup":
				var p struct {
					Code string `json:"code"`
				}
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					code := strings.TrimPrefix(p.Code, "https://chat.whatsapp.com/")
					gJID, err := client.JoinGroupWithLink(ctx, code)
					if err == nil {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok", "resp": gJID.String()})
					} else {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "error", "error": err.Error()})
					}
				}

			// Send Presence Update
			case "sendPresenceUpdate":
				var p struct {
					Presence string `json:"presence"`
					JID      string `json:"jid"`
				}
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					var presence types.Presence
					switch p.Presence {
					case "composing":
						presence = types.PresenceComposing
					case "recording":
						presence = types.PresenceMediaRecording
					case "paused":
						presence = types.PresencePaused
					case "available":
						presence = types.PresenceAvailable
					case "unavailable":
						presence = types.PresenceUnavailable
					default:
						presence = types.PresenceAvailable
					}
					targetJID, _ := types.ParseJID(p.JID)
					if targetJID.IsEmpty() {
						client.SendPresence(ctx, presence)
					} else {
						client.SendChatPresence(ctx, targetJID, presence, types.ChatPresenceMediaText)
					}
					sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok"})
				}

			// Update Profile Status (Bio)
			case "updateProfileStatus", "setStatus":
				var p struct {
					Status string `json:"status"`
				}
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					err := client.SetStatusMessage(ctx, p.Status)
					if err == nil {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok"})
					} else {
						sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "error", "error": err.Error()})
					}
				}

			// Read Messages
			case "readMessages":
				var p struct {
					Keys []struct {
						RemoteJID   string `json:"remoteJid"`
						ID          string `json:"id"`
						Participant string `json:"participant"`
					} `json:"keys"`
				}
				if err := json.Unmarshal(cmd.Payload, &p); err == nil {
					for _, k := range p.Keys {
						cJID, _ := types.ParseJID(k.RemoteJID)
						sJID, _ := types.ParseJID(k.Participant)
						client.MarkRead(ctx, []string{k.ID}, time.Now(), cJID, sJID)
					}
					sendIPC("response", map[string]interface{}{"id": cmd.ID, "status": "ok"})
				}

			default:
				sendIPC("response", map[string]interface{}{
					"id":     cmd.ID,
					"status": "error",
					"error":  "Aksi tidak dikenal: " + cmd.Action,
				})
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)

	signal.Notify(
		sigChan,
		os.Interrupt,
		syscall.SIGTERM,
	)

	<-sigChan

	fmt.Println(
		"\n[Go Engine] Menerima sinyal shutdown, menutup koneksi...",
	)

	if client.IsConnected() {
		client.Disconnect()
	}

	container.Close()
}