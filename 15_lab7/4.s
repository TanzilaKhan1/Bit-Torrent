    AREA |.rodata|, DATA, READONLY
; BCD data samples (each byte represents two BCD digits)
bcd_data     DCB    0x25, 0x37, 0x43       ; BCD: 25, 37, 43


    AREA |.data|, DATA, READWRITE
binary_ans   DCD    0, 0, 0             ; Storage for binary results


    AREA |.text|, CODE, READONLY
    ENTRY
    EXPORT main

main
    LDR R5, =bcd_data          ; R5 = address of BCD data
    LDR R6, =binary_ans        ; R6 = address of binary results
    MOV R7, #3                 ; Number of BCD values to convert
    MOV R8, #0                 ; Index counter
    
convert_loop
    CMP R8, R7
    BGE conversion_done

    LDRB R0, [R5, R8]          ; R0 = BCD value to convert
    BL BCD_binary
    STR R0, [R6, R8, LSL #2]   ; Store in binary_results[index]
    
    ADD R8, R8, #1             ; Increment index
    B convert_loop

conversion_done
    B stop

; Function: BCD_binary
; Input: R0 = BCD value (two digits packed in one byte)
; Output: R0 = Binary equivalent (e.g., 0x25 ? 0x19)

BCD_binary
    MOV     R1, R0, LSR #4     ; R1 = upper nibble (tens)
    AND     R2, R0, #0x0F      ; R2 = lower nibble (units)
    MOV     R3, #10
    MUL     R1, R1, R3         ; R1 = tens * 10
    ADD     R0, R1, R2         ; R0 = (tens * 10) + units
    BX      LR                 ; Return from function

stop
    B stop
    END