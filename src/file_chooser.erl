%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% This erlang program is escript compatible.
%%% Its purpose is to open a file dialog and receive a file name
%%% When called as an escript it will write its result on standard
%%% output for processing by other programs in a pipe.
%%% When used by other erlang programs it expects a wx session to be
%%% open and allows the dialog in a parent window.
%%% @end
%%% Created : 24 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(file_chooser).
-include_lib("wx/include/wx.hrl").

%% API
-export([main/1,get/1,get/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
main(_) ->
    wx:new(),
    %% type(Parent) = wxWindow:wxWindow()
    Parent = wx:null(),
    main_response(present_dialog(Parent)).

get() ->
    _Wx = wx:new(),
    FN = file_chooser:get(wx:null()),
    wx:destroy(),
    FN.

get(Parent) ->
    present_dialog(Parent).

present_dialog(Parent) ->
    FileDlg = wxFileDialog:new(Parent),
    maybe_get_filename(wxDialog:showModal(FileDlg),FileDlg).

maybe_get_filename(?wxID_CANCEL,_) ->
    none;
maybe_get_filename(_DlgReturnCode,FileDlg) ->
    wxFileDialog:getPath(FileDlg).
    
main_response(none) ->
    exit(-1);
main_response(FileName) ->
    io:format("~s~n",[FileName]).

    
    
%%%===================================================================
%%% Internal functions
%%%===================================================================
