
-module(tcp_udp).

-export([start/0]).


start()->
    spawn(fun()-> start_echo_server() end),
    spawn(fun()->  udp_demo_server() end).

start_echo_server()->
    {ok,Listen}= gen_tcp:listen(1234,[binary,{packet,4},{reuseaddr,true},{active,true}]),
    %%{ok,socket}=get_tcp:accept(Listen),
    %%gen_tcp:close(Listen),
    loop(Listen).
 
loop(Socket) ->
    receive
        _->
            io:format("wztest tcp .....")
    end.
    %%receive
    %%    {tcp,Socket,Bin} ->
    %%             io:format(“serverreceived binary = ~p~n”,[Bin]),
    %%             Str= binary_to_term(Bin),
    %%             io:format(“server  (unpacked) ~p~n”,[Str]),
    %%             Reply= lib_misc:string2value(Str),
    %%             io:format(“serverreplying = ~p~n”,[Reply]),
    %%             gen_tcp:send(Socket,term_to_binary(Reply)),
    %%             loop(Socket);
    %%    {tcp_closed,Socket} ->
    %%             io:format(“ServerSocket closed ~n”)
    %%end.



udp_demo_server() ->
     {ok,Socket}= gen_udp:open(1234,[binary]),
     loop_udp(Socket).

loop_udp(Socket)->
    receive
        _->
            io:format("wztest upd ....")
    end.
    %%receive
    %%    {udp,Socket,Host,Port,Bin}->
    %%             BinReply= …,
    %%             gen_udp:send(Socket,Host,Port,BinReply),
    %%             loop(Socket)
    %%end.
