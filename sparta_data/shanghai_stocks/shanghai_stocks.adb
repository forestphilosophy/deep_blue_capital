with Ada.Calendar.Formatting;

with Auction_States;
use  Auction_States;

with New_Lasts;
use  New_Lasts;

with Market_Phases;
use  Market_Phases;

with Market_Codes;
use  Market_Codes;

with Time_Zones;
use  Time_Zones;

with Sparta.Choosers;
use  Sparta.Choosers;

with Sparta.Dealers_Manager;
use  Sparta.Dealers_Manager;

with Sparta.Kinds;
use  Sparta.Kinds;

with Sparta.Stubs.Types.Ops;
use  Sparta.Stubs.Types.Ops;

with Sparta.Market_Data.Auctions;

with Sparta.Market_Data.Consumers.Stubs;
use  Sparta.Market_Data.Consumers.Stubs;

with Sparta.Market_Data.Phase_Support;
use  Sparta.Market_Data.Phase_Support;

with Sparta.TR.Metas.Stubs;
use  Sparta.TR.Metas.Stubs;

with Sparta.TR.Fid_Fields.Stubs;
use  Sparta.TR.Fid_Fields.Stubs;

with Sparta.TR.L2;
use  Sparta.TR.L2;

package body Sparta.TR.Exchange_Specifics.Shanghai_Stocks is

   use Sparta.Market_Data.Lasts;
   use Sparta.TR.Stub_Types.Enum_T;
   use Sparta.Stubs.Types.Duration_T;

   function TR_China_C1_Auction_State_Dealer
     (Compute : not null access constant Boolean_Stub_Descriptor_Type'Class)
      return Dealers.Dealer_Descriptor_Type_Access
   is (Market_Data.Auctions.Standard_L1_Auction_State_Dealer
       (TimeStamp => Received,
        Exchange_Stamp => Use_First(Combine(Received, Use_First(S_INDAUC_MS, S_IMB_TIM_MS)), Received),
        Akind =>
           Auction_State_Kind_Type_T_Logic.If_Then_Else
          (Phase <= Auction_Phase,
           Auction_State_Kind_Type_T_Logic.If_Then_Else
             (Is_Valid (B_IND_AUC) and
                  Is_Valid (B_IND_AUCVOL) and
                  not Equal (B_IND_AUC, 0.0) and
                  not Equal (B_IND_AUCVOL, 0.0),
              Auction_State_Kind_Type_T.Const (Known),
              Auction_State_Kind_Type_T_Logic.If_Then_Else
                ((Is_Valid (B_BEST_BID1) and
                            Is_Valid (B_BEST_BSIZ1) and
                            not Equal (B_BEST_BID1, 0.0) and
                            not Equal (B_BEST_BSIZ1, 0.0)) or
                   (Is_Valid (B_BEST_ASK1) and
                        Is_Valid (B_BEST_ASIZ1) and
                        not Equal (B_BEST_ASK1, 0.0) and
                        not Equal (B_BEST_ASIZ1, 0.0)),
                 Auction_State_Kind_Type_T.Const (Non_Tradable),
                 Auction_State_Kind_Type_T.Const (None))),
           Auction_State_Kind_Type_T.Const (None)),
        Has_Volume => S_True,
        Known_Price   => TR_Construct_Price_L1 (B_IND_AUC),
        Known_Volumes =>
          (Bid => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier *
                  (B_IND_AUCVOL
                   + If_Then_Else
                     (B_IMB_SIDE <= 2,
                      B_IMB_SH, Scalar_T.Const (0.0)))),
           Ask => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier *
                  (B_IND_AUCVOL
                   + If_Then_Else
                     (B_IMB_SIDE <= 3,
                      B_IMB_SH, Scalar_T.Const (0.0))))),
        Guessed_Price_Min => Price_Type_T.Not_Set_Stub,
        Guessed_Price_Max => Price_Type_T.Not_Set_Stub,
        Guessed_Volume    => Unsigned_Volume_Type_T.Not_Set_Stub,
        Non_Tradable_Prices  =>
          (Bid => TR_Construct_Price_L1 (B_BEST_BID1),
           Ask => TR_Construct_Price_L1 (B_BEST_BID1)),
        Non_Tradable_Volumes =>
          (Bid => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier * B_BEST_BSIZ1),
           Ask => Construct_Unsigned_Volume
             (Info_Volume_Lot_Multiplier * B_BEST_ASIZ1)),
        Meta    => A_Meta_L1,
        Compute => Compute,
        Assertions     =>
          (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
           Assert_Currency_Multiplier_L1,
           Assert_No_Auction_Price,
           Asserts.Boolean_Assert
             (A_L2_Only
              or Is_Valid (B_BEST_ASK1)
              or not Is_Valid (B_BEST_BSIZ1),
              "Auction BID volume with no Auction BID price",
              Kinds.Except),
           Asserts.Boolean_Assert
             (A_L2_Only
              or Is_Valid (B_BEST_ASK1)
              or not Is_Valid (B_BEST_BSIZ1),
              "Auction ASK volume with no Auction ASK price",
              Kinds.Except)
          )));

begin
   Set_Global_Chooser
     (Data_Vendor (TR.Reuters_Data_Vendor));
   -- !!!!!!!!     Add "Register" after this line.     !!!!!!!!!

   Register
     (Mkt (MKT_XSHG), Tag => "Shanghai_Stocks",
      Descriptors =>
        (TR_Standard_L1_Price_Dealer_S
             (Stamp   =>
                  If_Then_Else(
                Meta_Update <= EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN
                or
                  Meta_Class <= Standard.TR.Refresh or
                    not (Is_Valid(S_ASK) or Is_Valid(S_ASK) or
                    Is_Valid(S_BIDSIZE) or Is_Valid(S_ASKSIZE)),
                Use_First(B_QUOTE_DATE + Use_First(S_QUOTIM_NS, S_QUOTIM_MS), Received),
                B_QUOTE_DATE + Use_First(S_QUOTIM_NS, S_QUOTIM_MS)),
              Compute =>
                Meta_Class <= Standard.TR.Refresh or
                  Meta_Update <=
                (1 => EMA.RDM.INSTRUMENT_UPDATE_MARKET_DIGEST,
                 2 => EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN)),

         Tr_Standard_L1_Opening_Price_Dealer                                    --600874.SH around 2018-12-12T02:17:21.208526059
           (Stamp => Monotonize (Use_First(                                    --and some messages onwards, strange OPEN_PRC.
            Use_First(                                                          --first 9.29, then next message 9.32. Looks like
              Use_First(B_TRADE_DATE + S_SALTIM_MS, B_Quote_Date + S_QUOTIM_MS),--9.29 is wrong, because it uses exchtim and the next
              Combine(Received, S_EXCHTIM)),                                    --one saltim_ms we needed to monotonize.
            Received))),

         Tr_Standard_L1_Closing_Price_Dealer (Received),
         Combine_L2(
              L2_By_Price =>
              TR_L2_By_Price_Dealer(
              Summary_Stamp =>
                                  TR_Monotonize(TR_Stamp_Refresh_With_Received( -- 2020-01-07T01:40:44.862946928Z TH_603713.SS_t9_0x07ce2e47ab9d59ff_Full.csv.gz. time going backwards -> monotonize
                                    B_ACTIV_DATE_L2S + S_TIMACT_MS_L2S,         -- 2018-01-02T04:45:00.6813 AGL.MI_L2p.csv.gz. Need to use Tr_stamp_with_received
                                    B_ACTIV_DATE_L2S + B_TIMACT_MS_L2S,
                                    B_ACTIV_DATE_L2S + S_TIMACT_MS_L2S)),

               Entry_Stamp   => S_LV_DATE_L2E + S_LV_TIM_MS_L2E,

               Strictness => (Order_Book_Support.Patient                        -- Orders removed multiple times
                               with delta
                             Log_Delete  => Known_Bug,
                             Log_Add  => Known_Bug,
                             Log_Update => Known_Bug)),                        -- Orders updated after being already removed

             L2_By_Order =>
                TR_PST_L2_By_Order_Dealer(
               Summary_Stamp =>
                                 TR_Monotonize(Use_First(TR_Stamp_Refresh_With_Received(
                                  B_ACTIV_DATE_L2S+S_TIMACT_MS_L2S,         -- 2019-07-31T04:35:02.299 ENI.MI_L2o_2.csv.gz
                                  B_ACTIV_DATE_L2S+B_TIMACT_MS_L2S,         -- quote updates message is too big and is splitted
                                  B_ACTIV_DATE_L2S+S_TIMACT_MS_L2S),
                                  Complain(Received,
                                         "No summary timestamp supplied",
                                          Known_Bug)),
                                          Level=> Known_Bug),           -- and we do not have summary stamp on the second part

               Strictness => (Order_Book_Support.Patient                        -- Orders removed multiple times
                               with delta
                             Log_Delete  => Known_Bug,
                             Log_Add  => Known_Bug,
                             Log_Update => Known_Bug),

               Entry_Stamp   => S_ACTIV_DATE_L2S + S_PR_TIM_MS))));             --Updates.Null_Updates_Dealer));


   Register
     (Mkt (MKT_XSHG), Tag => "Shanghai_Stocks_to_2018-08-20",
      To    => Ada.Calendar.Formatting.Time_Of (2018, 8, 20),
      Descriptors =>
        (TR_Standard_L1_Phase_Dealer
             (Stamp => Use_First(Combine(Received, Use_First(S_QUOTIM_MS, S_INDAUC_MS)), Received),
              Compute => Changed (B_PERIOD_CDE) or Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:27:00")
              or Meta_Class <= Standard.TR.Refresh,
              Map => (Auction_Phase        => B_PERIOD_CDE <= "M111" or
                        ((Is_Blank(B_IND_AUC) or Is_Valid(B_IND_AUC)) and
                           Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:27:00") and not
                             (B_PERIOD_CDE <= "C111" + "D111")),                --period_cde all possible values case 06880466
                      No_Trading_Phase     => B_PERIOD_CDE <= "ENDT" + "C111" + "B111" + "E111" + "D111" + "E011" + "S 11" and not
                        ((Is_Blank(B_IND_AUC) or Is_Valid(B_IND_AUC)) and
                           Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:27:00") and not
                             (B_PERIOD_CDE <= "C111" + "D111")),
                      Trading_Phase        => B_PERIOD_CDE <= "T111",
                      others               => null)),
         TR_Standard_L1_Last_Dealer
           (Stamp      => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),
            Last_Stamp => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),
            Flag_Map   =>
              (Normal_Trade               => S_AGGRS_SID1 <= 1 & 2,
               Interruption_Auction_Trade => S_AGGRS_SID1 <= 0 and S_PERIOD_CDE <= "M111" + "T111",
               Opening_Auction_Trade      => S_AGGRS_SID1 <= 0 and
                 (B_PERIOD_CDE <= "D111" + "C111" or ((not Is_Valid(B_PERIOD_CDE) or B_PERIOD_CDE <= "ENDT" + "E111" + "E011" + "S 11") and
                      Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:26:00"))), --intraday trade?
               others               => null)),
         TR_China_C1_Auction_State_Dealer
           ( A_L1_Only and
                (Meta_Class <= Standard.TR.Refresh
                 or
                   Meta_Update <= (EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN,
                     EMA.RDM.INSTRUMENT_UPDATE_TRADE))),
         Standard_Aggregated_Last_Dealer
           (Stamp        => Received,
            Meta         => A_Meta_L1,
            When_Release => (Auction_Trades =>
                                 Delayed_After_True
                               (D       => 10.0,
                                C       => S_AGGRS_SID1 <= 0 and
                                  (S_PERIOD_CDE <= "D111" + "C111" or
                                       ((not Is_Valid(S_PERIOD_CDE) or S_PERIOD_CDE <= "ENDT" + "E111" + "E011" + "S 11") and
                                            Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:26:00"))),
                                Stamp   => Received),
                             Normal_Trade => Delayed_After_True
                               (D       => 10.0,
                                C       => not (Phase <= Trading_Phase),
                                Stamp   => Received),
                             others => null))
        ));

   Register
     (Mkt (MKT_XSHG), Tag => "Shanghai_Stocks_from__2018-08-20",
      From    => Ada.Calendar.Formatting.Time_Of (2018, 8, 20),
      Descriptors =>
        (TR_Standard_L1_Phase_Dealer
             (Stamp => Use_First(Combine(Received, Use_First(S_QUOTIM_MS, S_INDAUC_MS)), Received),
              Compute => A_Phase_Compute or
                Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:31:00") or
                  Contained (Time_Of_Day (Received, CST_TZ), +"11:29:00", +"13:01:00"),
              Map => (Auction_Phase        => B_INST_PHASE <= 4 & 6 or ((Is_Blank(B_IND_AUC) or Is_Valid(B_IND_AUC)) and
                        Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:27:00") and not
                          (B_INST_PHASE <= 0 & 3)),
                      No_Trading_Phase     => (not Is_Valid(B_INST_PHASE)) or (B_INST_PHASE <= 1 and not ((Is_Blank(B_IND_AUC) or Is_Valid(B_IND_AUC)) and
                                                 Contained (Time_Of_Day (Received, CST_TZ), +"09:14:00", +"09:27:00"))) or
                          (B_INST_PHASE <= 3 and
                             (Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:30:00") or
                                Contained (Time_Of_Day (Received, CST_TZ), +"11:30:00", +"13:00:00"))),
                      Trading_Phase        => B_INST_PHASE <= 3 and not
                        (Contained (Time_Of_Day (Received, CST_TZ), +"09:25:00", +"09:30:00") or
                           Contained (Time_Of_Day (Received, CST_TZ), +"11:30:00", +"13:00:00")),
                      Unknown_Phase => B_INST_PHASE <= 0,
                      others               => null)),
         TR_China_C1_Auction_State_Dealer
           (Compute =>
                A_L1_Only and
              (Meta_Class <= Standard.TR.Refresh
               or
                 Meta_Update <= (EMA.RDM.INSTRUMENT_UPDATE_CLOSING_RUN,
                   EMA.RDM.INSTRUMENT_UPDATE_MARKET_DIGEST))),
         TR_Standard_L1_Last_Dealer
           (Stamp      => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),   --duplicate messages time move backwards
            Last_Stamp => B_TRADE_DATE + Use_First(S_SALTIM_NS, S_SALTIM_MS),   --2019-10-23T01:54:02.102918
            Compute    => A_Last_Compute and
              Boolean_T.Set_Label (Is_Increasing (S_IRGVOL, Is_Blank(S_IRGVOL)),
                                   "IRGVOL_INCREASING"),
            Ignore     => A_L1_Ignore or not Labelled ("IRGVOL_INCREASING"),
            Assertions => (Assert_Currency_L1, Assert_Currency_Blank_Or_Zero_L1,
                           Assert_Currency_Multiplier_L1,
                           Assert_SEQNUM_Ordering_L1 (Known_Bug)
                          ),
            Flag_Map =>
              (Normal_Trade               => S_AGGRS_SID1 <= 1 & 2 and B_INST_PHASE <= 3 & 6, -- TH_e222791-r693_t8_0x07dc30b7ad4d7944_Full.csv.gz 2019-02-27T06:57:03.069249703Z continous trades are received after the closing auction phase
               Interruption_Auction_Trade => S_AGGRS_SID1 <= 0 and B_INST_PHASE <= 4,
               Opening_Auction_Trade      => S_AGGRS_SID1 <= 0 and B_INST_PHASE <= 2 & 3,
               Closing_Auction_Trade      => S_AGGRS_SID1 <= 0 and B_INST_PHASE <= 1 & 6,
               others                     => null)),

         Standard_Aggregated_Last_Dealer
           (Stamp        => Received,
            Meta         => A_Meta_L1,
            When_Release => (Auction_Trades => not (S_AGGRS_SID1 <= 0),
                             Normal_Trade => Delayed_After_True
                               (D       => 10.0,
                                C       => not (B_INST_PHASE <= 3),
                                Stamp   => Received),
                             others => null))));


   -- !!!!!!!!     Add "Register" before this line.     !!!!!!!!!
   Set_Global_Chooser (null);
end Sparta.TR.Exchange_Specifics.Shanghai_Stocks;
