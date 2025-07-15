; --- Configuration ---
CPU_SPEED_MHZ    EQU      12       ; Keil Simulator's Default Clock Speed is 12 MHz

    AREA |.data|, DATA, READWRITE
BcdCounter       DCB      0x00     ; The BCD counter, starts at 0

    AREA |.text|, CODE, READONLY
    ENTRY
    EXPORT main

main
	LDR     R0, =BcdCounter      ; R0 = Address of the BCD counter

Main_Loop
    ; Step 1: Increment the BCD counter
    LDRB    R1, [R0]             ; Load the current BCD value
    BL      BCD_Increment        ; increment R1 in BCD format
    STRB    R1, [R0]             ; Store the new BCD value back to memory

    ; Step 2: Call the 1-second delay
    BL      Delay_1_Sec

    ; Step 3: Repeat
    B       Main_Loop

;------------------------------------------------------------------------------
; Function: BCD_Increment
; Increments a BCD value by 1, with correct BCD carry logic.
; Input:  R1 = BCD value
; Output: R1 = BCD value + 1
;------------------------------------------------------------------------------

BCD_Increment
    ADD     R1, R1, #1           

    ; Check if the lower nibble needs decimal adjustment
    AND     R2, R1, #0x0F        ; Keep only lower nibble
    CMP     R2, #0x0A            ; Compare with 10 (0xA)
    BNE     Check_Upper_Nibble   ; If not 10, no adjustment needed
    ADD     R1, R1, #6           ; If it is 10 (e.g., 0x09 -> 0x0A), add 6 to fix


Check_Upper_Nibble
    ; Check if the upper nibble needs decimal adjustment
    CMP     R1, #0xA0               ; Has the value reached or exceeded 100? (0xA0)
    BLT     Done_Increment          ; If less than 0xA0, we are done
    ADD     R1, R1, #0x60           ; If it is 100 (e.g., 0x99 -> 0xA0), add 0x60


Done_Increment
    ; For the 99 -> 00 case, the result is 0x100 but want the 0x00
    AND     R1, R1, #0xFF           ; Mask to lower 8 bits.
    BX      LR                      


;------------------------------------------------------------------------------
; Function: Delay_1_Sec
; Wastes CPU cycles to create a delay of approximately 1 second.
; Assumes each loop iteration takes ~4 clock cycles.
;------------------------------------------------------------------------------

Delay_1_Sec
    ; Calculate the number of loops needed for 1 second.
    ; (Clock Speed in Hz) / (Cycles per loop)
    LDR     R3, =(CPU_SPEED_MHZ * 1000000 / 4)


Delay_Loop
    SUBS    R3, R3, #1              ; Decrement counter
    BNE     Delay_Loop              ; Loop until counter is zero
    BX      LR                      

    END