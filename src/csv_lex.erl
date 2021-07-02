%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% This server process is demand driven.  It is initialised with a
%%% process id of an input process which returns text chunks when
%%% prompted by a read message.
%%%
%%% This server produces tokens when prompted by a read message.
%%% on receipt of a read it attempts to tokenise its current input
%%% string to return a token, if it needs more data to do so, it calls 
%%% its input process to deliver more data.
%%% 
%%% Options are:
%%%  {date_format,date_format()}
%%%  where date_format() :: "DD/MM/YYYY"|"MM/DD/YYYY"
%%%
%%% The parser also allows event callbacks.  These callbacks are
%%% token_callback and eoln_callback.  At the end of a line the 
%%% token callback is executed before the eoln callback.
%%% Each callback receives {reply,Msg,State} as its parameter,
%%% and replies with an updated state.  Callbacks may store data
%%% that pertains to their operation is the map.
%%% @end
%%% Created : 17 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(csv_lex).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

-define (debug_level,1).
-if (?debug_level==2).
    -define (debug2(Fmt,Data),io:format(Fmt,Data)).
-else.
    -define (debug2(Fmt,Data),ok).
-endif.

%%-type date_format() :: "DD/MM/YYYY" | "MM/DD/YYYY".
%-type row() :: pos_integer().
%-type col() :: pos_integer().
%-type token_type() :: date | string | number | empty.
%-type token() :: {token,row(),col(),{token_type(),any()}}.
%-type tokenizer_states() :: ready | quoted | accumulating | eof.
%-type storage_type() :: list() | tuple() | map().
%-type update_function() :: fun((storage_type) -> storage_type()).
%%%===================================================================
%%% API
%%%===================================================================


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------
-spec start_link({pid(),string()},list()) -> {ok, Pid :: pid()} |
		      {error, Error :: {already_started, pid()}} |
		      {error, Error :: term()} |
		      ignore.

start_link(X=#{input := _InputProc,
	       date_format := _DateFormat,
	       read_timeout := _ReadTimeout},Options) ->
    gen_server:start_link(?MODULE, X, Options).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
			      {ok, State :: term(), Timeout :: timeout()} |
			      {ok, State :: term(), hibernate} |
			      {stop, Reason :: term()} |
			      ignore.
init(S0=#{date_format := DateFormat}) ->
    % process_flag(trap_exit, true),
    case check_date_format(DateFormat) of
	ok ->
	    {ok, S0#{eof_input => false, 
		     buffer => [],
		     token_data => [],  % data for token assembled here
		     tokenizer_state => ready,
		     message_queue => queue:new(),
		     row => 0, 
		     col => 0,
		     seq => 0,
		     inquotes => false}};
	error ->
	    {stop, {incorrect_date_format,DateFormat}}
    end;
init(A) ->
    io:format("Unmatched input parameter ~p~n",[A]),
    {stop,{init_mismatch, A}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
			 {reply, Reply :: term(), NewState :: term()} |
			 {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
			 {reply, Reply :: term(), NewState :: term(), hibernate} |
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
			 {stop, Reason :: term(), NewState :: term()}.
handle_call(read, _From, State=#{message_queue := Q0}) ->
    case queue:out(Q0) of
	{empty,Q0} ->
	    get_token(State);
	{{value,V},Q1} ->
	    {reply,V,State#{message_queue := Q1}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), NewState :: term()}.
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
		  State :: term(),
		  Extra :: term()) -> {ok, NewState :: term()} |
				      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
		    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%% get_token returns a valid gen_server:handle_call response
get_token(S0=#{buffer := "", input := Input, 
	       read_timeout := ReadTimeout, token_data := TD}) ->
    case gen_server:call(Input,read,ReadTimeout) of
	eof -> 
	    %% there is no requirement that there be an end of line
	    %% on the last data line. So need to perform token and
	    %% end of line processing in this case.  Thus an eof causes:
	    %% (1) The current token to be processed and returned to as
	    %% the result of the gen_server:call.
	    %% (2) A line message to be added to the output queue. 
	    %% This is done by line_processing
	    %% (3) An eof message to be added to the output queue.
	    case TD of
		[] ->
		    {stop,normal,eof,S0};
		_ -> 
		    {reply,Token,S1} = line_processing(S0),
		    #{message_queue := MQ} = S1,
		    S2 = S1#{message_queue := queue:in(eof,MQ)},
		    {reply,Token,S2}
	    end;
	Data ->
	    ?debug2("csv_lex:got Data ~p~n",[Data]),
	    get_token(S0#{buffer := Data})
    end;
	    
get_token(S0=#{tokenizer_state := ready, buffer := [$"|Rest]}) ->
    accumulate_token(S0#{tokenizer_state := quoted, 
			 buffer := Rest,
			 token_data := []});
get_token(S0=#{tokenizer_state := ready}) ->
    accumulate_token(S0#{tokenizer_state := accumulating, 
			 token_data := []});
get_token(S0) ->
    throw ({"get_token called while not in ready state",S0}).

%% accumulate_token returns a valid gen_server response
%% at the time of the return it must be in ready or eof tokenizer
%% state.
accumulate_token(S0=#{buffer := [],
		      input := Input,
		      read_timeout := ReadTimeout}) ->
    ?debug2("accumulate_token pattern 1~n",[]),
    Reply = gen_server:call(Input,read,ReadTimeout),
    ?debug2("Term ~p received from source~n",[Reply]),
    
    case Reply of
	eof ->
	    token_processing(S0);
	Data ->
	    accumulate_token(S0#{buffer := Data})
    end;

%% quoted state takes priority.  Commas, newlines etc
%% are all accepted as data in quoted state
%% 
accumulate_token(S0=#{buffer := [$",$"|Rest], 
		     tokenizer_state := quoted,
		     token_data := TD}) ->
    ?debug2("accumulate_token pattern 2~n",[]),
    accumulate_token(S0#{buffer := Rest, token_data := [TD,$"]});
accumulate_token(S0=#{buffer := [$",$,|Rest],
		      tokenizer_state := quoted}) ->
    %% valid termination of quoted data
    ?debug2("accumulate_token pattern 3~n",[]),
    token_processing(S0#{buffer := Rest});
accumulate_token(S0=#{buffer := [$",10|Rest],
		      tokenizer_state := quoted}) ->
    ?debug2("accumulate_token pattern 4~n",[]),
    line_processing(S0#{buffer := Rest});
accumulate_token(S0=#{buffer := [$",13,10|Rest],
		      tokenizer_state := quoted}) ->
    ?debug2("accumulate_token pattern 5~n",[]),
    line_processing(S0#{buffer := Rest});
accumulate_token(S0=#{buffer := [Char|Rest],
		      tokenizer_state := quoted,
		      token_data := TD}) ->
    ?debug2("accumulate_token pattern 6~n",[]),

    accumulate_token(S0#{buffer := Rest, token_data := [TD, Char]});
%% under no circumstances should accumulate token get here in the
%% quoted state
accumulate_token(S0=#{tokenizer_state := quoted}) ->
    ?debug2("accumulate_token pattern 7~n",[]),
    throw ({accumulate_token_assertion_fail,S0});
%% rules for accumulate token not in quoted state
%% check for end of line
accumulate_token(S0=#{buffer := [13,10|Rest]}) ->
    ?debug2("accumulate_token pattern 3~n",[]),
    line_processing(S0#{buffer := Rest});

accumulate_token(S0=#{buffer := [10|Rest]}) ->
    ?debug2("accumulate_token pattern 8~n",[]),
    line_processing(S0#{buffer := Rest});
accumulate_token(S0=#{buffer := [$,|Rest]}) ->
    ?debug2("accumulate_token pattern 9~n",[]),
    token_processing(S0#{buffer := Rest});
accumulate_token(S0=#{buffer := [Char|Rest],
		     token_data := TD}) ->
    ?debug2("accumulate_token pattern 10~n",[]),
    accumulate_token(S0#{buffer := Rest, token_data := [TD,Char]}).

%% Note that token processing occurs before line processing
%% As the token that was accumulating on that line, belongs 
%% on that line.
line_processing(S0=#{row:=Row}) ->
    {reply,Msg,S1} = token_processing(S0),
    #{message_queue := MQ} = S1,
    S2 = S1#{message_queue := queue:in({line,Row},MQ)},
    {reply,Msg,S2#{row := Row+1,col:=0}}.

%% token processing must return a valid genserver response
token_processing( S0=#{token_callback := Callback}) ->
    R1={reply,Msg,_} =token_processing2(S0),
    S2 = Callback(R1),
    {reply,Msg,S2};
token_processing(S0) ->
    token_processing2(S0).

token_processing2(S0=#{token_data := "", col:=Col, seq:=Seq, row:=Row}) ->
    {reply,
     #{token => null,
       row => Row,
       col => Col,
       seq => Seq},
     S0#{tokenizer_state:=ready,
	 seq:=Seq+1,
	 col:=Col+1}};
    
token_processing2(S0=#{token_data := TD, col:=Col, seq:=Seq, row:=Row}) ->
    ?debug2("token processing, token=~p~n",[TD]),
    {Token,_} = tp1(lists:flatten(TD),S0),
    {reply,
     #{token => Token,
       row => Row,
       col => Col,
       seq => Seq},
     S0#{tokenizer_state:=ready,
	 token_data := "",
	 seq:=Seq+1,
	 col:=Col+1}}.

tp1(Data,State=#{date_format := DateFormat}) ->
    ?debug2("tp1 token ~p~n",[Data]),
    tp2(check_is_date(Data,DateFormat),Data,State).
tp2({date,Data},_,State) ->
    {{date,Data},State};
tp2(false,Data,State) ->
    tp3(check_is_number(Data),Data,State).
tp3({true,Type,Value},_,State) ->
    {{Type,Value},State};
tp3(false,Data,State) ->
    %% for effiency, return string as binary
    {{string,list_to_binary(Data)},State}.

check_is_date(Str,Fmt) ->
    case check_is_date2(pattern,Fmt,Str) of
	true ->
	    ?debug2("check_is_date: delimiter pattern ok~n",[]),
	    case to_date(Str,Fmt) of
		{true,Date} ->
		    {date,Date};
		{false,_} ->
		    ?debug2("check_is_date: not a valid date~n",[]),
		    false
	    end;
	false ->
	    false
    end.


check_is_date2(pattern,"YYYY/MM/DD",Str=[_,_,_,_,$/,_,_,$/,_,_]) ->
    check_is_date3(digits,Str);
check_is_date2(pattern,_Fmt,Str=[_,_,$/,_,_,$/,_,_,_,_]) ->
    check_is_date3(digits,Str);
check_is_date2(pattern,_,_) ->
    false.

check_is_date3(digits,[]) ->
    true;
check_is_date3(digits,[A|T]) when A >= $0, A =< $9 ->
    check_is_date3(digits,T);
check_is_date3(digits,[$/|T]) ->
    check_is_date3(digits,T);
check_is_date3(_,_) ->
    false.

%% a number is an optional sign, an integer part, optional decimal point, and a fractional part
check_is_number(Str) ->
    check_is_number(split,string:split(Str,"."),Str).
check_is_number(split,[PossibleInteger],Str) ->
    check_is_number(possible_integer,string:to_integer(PossibleInteger),Str);
check_is_number(split,[_,_],Str) ->
    check_is_number(possible_real,string:to_float(Str),Str);
check_is_number(possible_integer,{X,[]},_) ->
    {true,integer,X};
check_is_number(possible_real,{X,[]},_) ->
    {true,float,X};
check_is_number(_,_,_) ->
    false.

check_date_format("DD/MM/YYYY") ->
    ok;
check_date_format("MM/DD/YYYY") ->
    ok;
check_date_format("YYYY/MM/DD") ->
    ok;
check_date_format(_) ->
    error.

to_date(Str,Fmt) ->
    DateParts = string:split(Str,"/",all),
    FmtParts = string:split(Fmt,"/",all),
    Aligned = lists:zip(FmtParts,DateParts),
    ?debug2("to_date:aligned = ~p~n",[Aligned]),
    ConvertedToInt = [{FmtPart,string:to_integer(DatePart)}
		     || {FmtPart,DatePart} <- Aligned],
    ?debug2("to_date:convertedToInt = ~p~n",[ConvertedToInt]),
    [Year] = [ YearInt || {"YYYY",{YearInt,_}} <- ConvertedToInt ],
    [Mth] = [M  || {"MM",{M,_}} <- ConvertedToInt ],
    [Day] = [D || {"DD",{D,_}} <- ConvertedToInt ],
    Date = {Year,Mth,Day},
    {calendar:valid_date(Date),Date}.
