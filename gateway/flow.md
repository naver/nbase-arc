* User Client Accept

```
    [Master]                                                   [Worker]

    user_accept_tcp_handler() ----------------------------> add_client()
         <gw_main.c>                async_send_event        <gw_main.c>
                                    <gw_async_event.c>          |
                                                                V
                                                            cli_add_client()
                                                            <gw_client.c>
                                                                |
                                                                V
                                                            {create readable event}
                                                            cli_read_handler()
                                                            <gw_client.c>
```

* Admin Client Accept

```
    [Master]

    admin_accept_tcp_handler()
    <gw_main.c>
        |
        V
    cli_add_client()
    <gw_client.c>
        |
        V
    {create readable event}
    cli_read_handler()
    <gw_client.c>
```

* User Command Example (Worker)

```
    [gw_client]                         [gw_cmd_mgr]                       [gw_redis_pool]

    {readable event fire}
    cli_read_handler()
         |
         V
       read()
         |
         +--------------------------->  cmd_send_command()
                                            |
                                            |
                                            V
                                        single_key_command()  --------->  pool_send_query(slot_idx)
                                        <gw_cmd_redis.c>                        |
                                                                                V
                                                                          slot_select_conn()
                                                                                |
                                                                                V
                                                                          add_msg_to_send_q()
                                                                                |
                                                                                V
                                                                          {create writable event}
                                                                          msg_send_handler()

                                                                               ...

                                                                          {writable event fire}
                                                                          msg_send_handler()
                                                                                |
                                                                                V
    cli_reply_handler() <-------------  single_key_command()                write()   ----------------------+
         |                              <gw_cmd_redis.c>
         V                                  ^                                  ...                          |
    {create writable event}                 |                                                               V
    cli_send_handler()                      |                             {readable event fire}        Redis Server
                                            |                             msg_read_handler()                |
        ...                                 |                                   |                           |
                                            |                                   V                           |
    {writable event fire}                   |                               read()   <----------------------+
    cli_send_handler()                      |                                   |
         |                                  |                                   V
         V                                  |                             process_msg_success()
      write()                               |                                   |
                                            |      msg_reply_cb                 V
                                            +--------------------------   process_msg_finish()
```


* Admin Command Example

```
    [master, gw_client]                 [master, gw_cmd_mgr]                [worker, gw_cmd_mgr]        [worker, gw_redis_pool]

    {readable event fire}
    cli_read_handler()
         |
         V
       read()
         |
         +--------------------------->  cmd_send_command()
                                            |
                                            |
                                            V
                                        admin_delay_command()
                                        <gw_cmd_admin.c>
                                            |
                                            V
                                        parse_delay_cmd()
                                            |
                                            V
                                        check_delay_cmd()
                                            |
                                            V
                                        send_delay_cmd() ---------------> exec_delay_cmd()
                                                         async_send_event       |
                                                                                +----------------------> pool_block_slot()

                                                                                                              ...

                                                                                    block_complete_cb    {after block complete}
                                                                                +----------------------  process_block_complete()
                                                                                |
                                                                                V
                                            +---------------------------- reply_delay_cmd()
                                            |            async_send_event
                                            V
                                        admin_delay_command()
                                            |
                                            V
                                        post_delay_cmd()
                           cmd_reply_cb     |
    cli_reply_handler() <------------------ +
         |
         V
    {create writable event}
    cli_send_handler()

        ...

    {writable event fire}
    cli_send_handler()
         |
         V
      write()
```
