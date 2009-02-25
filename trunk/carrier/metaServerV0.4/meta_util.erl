%% Author: zyb@fit
%% Created: 2008-12-22
%% Description: TODO: Add description to util
-module(meta_util).
-compile(export_all).

%%
%% Include files
%%

-define(NUM_CHR_UPPER,
{$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$A,$B,$C,$D,$E,$F,$G,$H,$I,$J,$K,$L,$M,$N,$O,$P,$Q,$R,$S,$T,$U,$V,$W,$X,$Y,$Z}).
-define(NUM_CHR_LOWER,
{$0,$1,$2,$3,$4,$5,$6,$7,$8,$9,$a,$b,$c,$d,$e,$f,$g,$h,$i,$j,$k,$l,$m,$n,$o,$p,$q,$r,$s,$t,$u,$v,$w,$x,$y,$z}).


%%
%% Exported Functions
%%
-export([for/3,sleep/1]).

%%
%% API Functions
%%

for(Max, Max, F) -> [F(Max)];
for(I, Max, F)   -> [F(I)|for(I+1, Max, F)].

%%
%% Local Functions
%%

for_test() ->    
    util:for(1,10,fun(I)->I*I end).


getNthList(Max, Max, List) ->  [Head | _] = List,
                               Head;
getNthList(I, Max, List)   ->  [_| Tail] = List,
                                    getNthList(I+1, Max, Tail).


idToAtom(Bin,Mode)->
    List = binary_to_list(Bin),
%%     ModePreFix = binary_to_list(term_to_binary(Mode)),
    case Mode of 
        r ->
            ModePreFix = $r;
        w ->
            ModePreFix = $w;
        _ ->
            ModePreFix = $?    
    end,    
    ModeList = lists:append(ModePreFix,List),
    list_to_atom(ModeList).

generate_processname(Filename,Mode)->
    ModePreFix= case Mode of 
					r -> r@@@;
					w -> w@@@;
					a -> a@@@		    
				end,    
    ModeList = lists:append(atom_to_list(ModePreFix),Filename),
    list_to_atom(ModeList).




gen_guid() ->
    TimeIn100NanosBeg = calendar:datetime_to_gregorian_seconds({{1582, 10, 15}, {0, 0, 0}}) * 10000000,
    {_MegaSecs, _Secs, MicroSecs} = now(),
    TimeIn100NanosNow =
        calendar:datetime_to_gregorian_seconds(calendar:universal_time()) * 10000000 + MicroSecs * 10,
    Time = TimeIn100NanosNow - TimeIn100NanosBeg,
    TimeStr = number_to_based_str(Time, 16, 2),
    TimeStr1 =
      case 15 - length(TimeStr) of
          Rem when Rem > 0 -> lists:duplicate(Rem, $0) ++ TimeStr;
          _                -> TimeStr
      end,
    TimeHiV = lists:sublist(TimeStr1, 1, 3) ++ "1", %% add version number 1,
    TimeMid = lists:sublist(TimeStr1, 4, 4),
    TimeLow = lists:sublist(TimeStr1, 8, 8),
    ClockSeqHiV = io:format("~2.16.0b", [(random:uniform(256) - 1)
                                        band 16#3f bor 16#80]), %% multiplexed variant type (2 bits)
    ClockSeqLow = io:format("~2.16.0b", [(random:uniform(256) - 1)]),
    Node = "001b631ee26b", %% an Ethernet MAC
    
    TimeLow ++ "-" ++ TimeMid ++ "-" ++ TimeHiV ++ "-" ++ ClockSeqHiV++ ClockSeqLow.
%% ++ "-" ++ Node.




number_to_based_str(Number, Base) -> number_to_based_str(Number, Base, []).
number_to_based_str(Number, Base, Digits) when is_integer(Digits) ->
    Str = number_to_based_str(Number, Base, []),
    case Digits - length(Str) of
        Rem when Rem > 0 -> lists:duplicate(Rem, $0) ++ Str;
        _                -> Str
    end;
number_to_based_str(Number, Base, _Acc) when Number < Base ->
    [element(Number + 1, ?NUM_CHR_LOWER)];
number_to_based_str(Number, Base,  Acc) ->
    Msd = Number div Base,
    Lsd = Number rem Base,
    case Msd >= Base of
        true  -> number_to_based_str(Msd, Base, [element(Lsd + 1,
?NUM_CHR_LOWER)|Acc]);
        false -> [element(Msd + 1, ?NUM_CHR_LOWER)|[element(Lsd + 1,
?NUM_CHR_LOWER)|Acc]]
    end.

sleep(T) ->
    receive
    after T -> true
    end.


host(Name)->
    Pid = spawn(util,pa,[Name]),
    global:register_name(Name,Pid).

client(Name)->
    io:format("~nsend, asdf~n"),
    global:send(Name,"asdf"),
    sleep(1000),
    
    io:format("~nsend, live~n"),
    global:send(Name,live),
    sleep(1000),
    
    io:format("~nsend, kill~n"),
    global:send(Name,kill),
    
    sleep(1000),
    io:format("~nsend, die~n"),
    global:send(Name,die).
    

pa(Name)->
    process_flag(trap_exit,true),
    wait(Name).

wait(Prog) ->
    receive
        exit ->
            true;
        live ->
            io:format("aaaaaa live,,~n"),
            wait(Prog);
        _Any ->
            io:format("Process ~p received ~p~n",[Prog, _Any]),
	    	wait(Prog)
    end.

c()->
    io:format("compiling file : metaDB~n"),
   	c:c(metaDB),
    io:format(".....ok~n"),
    io:format("compiling file : metagenserver~n"),
   	c:c(metagenserver),
    io:format(".....ok~n"),
    io:format("compiling file : metaserver~n"),
   	c:c(metaserver),
	io:format(".....ok~n"),
	io:format("compile finished.").

